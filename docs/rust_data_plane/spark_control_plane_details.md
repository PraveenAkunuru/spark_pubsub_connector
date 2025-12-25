# Spark Control Plane: Details & Patterns

This document details the Scala implementation and Spark DataSource V2 integration.

## 1. Architectural Overview
The Control Plane (in Scala) orchestrates Spark jobs and manages the JNI bridge:
- **DataSource V2**: Implements `TableProvider`, `MicroBatchStream`, and `DataWriter`.
- **JNI Proxies**: `NativeReader` and `NativeWriter` provide Scala wrappers for the Rust runtime.
- **Resource Management**: Uses standard Spark lifecycle hooks (`close()`, `commit()`) to manage native pointers and Arrow memory.

## 2. Structured Streaming Integration
The connector uses the Micro-Batch model for ingestion.

### 2.1. Offset Management
Currently implemented using a simple long-based counter (`PubSubOffset`). This tracks the progress of micro-batches but is decoupled from Pub/Sub's internal cursors (which are managed by the `StreamingPull` lease).

### 2.2. Virtual Partitioning (Read Parallelism)
To scale ingestion, the connector plans multiple parallel tasks even if reading from a single sub.
- **`planInputPartitions`**: Reads the `numPartitions` option (default: 1) and generates `N` independent partitions.
- **Executor Scaling**: Each Spark task on an executor initiates its own `NativeReader`.
- **Load Balancing**: Google Cloud Pub/Sub automatically balances messages across these parallel connections.

## 3. Reliability: Signal Propagation (Ack-on-Commit)
The connector implements an **At-Least-Once** delivery guarantee using a **Signal Propagation** pattern. This avoids Driver overload and JVM GC pressure by keeping ack metadata off-heap on executors.

### 3.1. The Data Carrier (Hidden Column)
- **Pre-FFI Capture**: To ensure strictly At-Least-Once delivery while avoiding memory double-frees and JVM heap pressure, the connector captures `ack_id`s within the native `getNextBatch` call. Before the message data is converted and exported to Arrow FFI, the Rust layer stores the `ack_id`s for the current batch in an **Off-Heap Native Reservoir**.
- **Integrated Storage**: This ensures that `ack_id` metadata is safely captured by the native state before it is handed off to Spark's internal engine.
- **Projection**: The `ack_id` column remains available in the Arrow batch but is typically projected out before processing.

### 3.2. Lifecycle Commit (Signal Propagation Implementation)
To achieve strictly At-Least-Once delivery, acknowledgments are triggered by a signal from the Driver:
1.  **Executor Reservoir**: In `PubSubPartitionReader.fetchNextBatch()`, the code calls `reader.getNextBatch(nativePtr, partition.batchId, ...)`. Inside this call, Rust captures and saves all `ack_ids` for that batch into the **Off-Heap Native Reservoir** before returning the data to Spark.
2.  **Driver Tracking**: In `PubSubMicroBatchStream.commit(end: Offset)`, the Driver adds the committed `batchId` to a persistent `pendingCommits` set.
3.  **Propagation**: In the next planning cycle (`planInputPartitions`), the Driver includes all `pendingCommits` in the `PubSubInputPartition` metadata.
4.  **Acknowledgment (Executor)**: When an Executor initializes a new partition reader, it receives the `committedBatchIds` signal. It immediately calls the native `ackCommitted` method, which flushes the corresponding IDs from the native reservoir and sends them to Pub/Sub.
5.  **Reliability**: If a failure occurs before the Driver's commit, no "Commit Signal" is ever generated. The messages remain in the native reservoir (or are lost if the executor crashes) and will be re-delivered by Pub/Sub after the Ack Deadline expires.
6.  **Scalability**: This design ensures zero GC impact on the Driver and eliminates the need for a shared filesystem for ack side-channels. It is verified by `AckIntegrationTest`.

## 4. Write Path (Publishing)
The write path uses `PubSubDataWriter` to buffer Spark rows.
- **Batching**: Rows are accumulated into a `VectorSchemaRoot` until `batchSize` is reached or `lingerMs` has elapsed since the first row of the batch.
- **Async Flush**: On `flush()` or `commit()`, the connector:
  1.  **Sets Row Count**: Finalizes the `VectorSchemaRoot` state.
  2.  **Exports**: Transfers ownership of the Arrow buffer to C-compatible structs (`ArrowArray`, `ArrowSchema`) via `exportVectorSchemaRoot`.
  3.  **JNI Call**: Invokes `NativeWriter.writeBatch`, passing the memory addresses.
  4.  **Reset**: Closes the exported root (decrementing Java's refcount) and creates a *new* root for the next batch, ensuring no shared state with the async Rust publisher.
- **Reference Counting**: The Java side uses `exportVectorSchemaRoot`, which increments reference counts. The critical pattern is that Java *must* close its handle to the root immediately after export, while Rust (via the release callback) manages the underlying memory lifecycle until the publish is complete.

## 5. Metadata and Utilities
- **`ArrowUtils`**: Centralized mapping logic for all Spark SQL types to Arrow vectors. 
  - **Read**: `getValue` extracts data from `FieldVector`.
  - **Write**: `setValue` populates vectors from `InternalRow`.
  - **Type Matrix**:
    | Spark Type | Arrow Type | Vector Class | Node |
    | :--- | :--- | :--- | :--- |
    | `Binary` | `Binary` | `VarBinaryVector` | |
    | `String` | `Utf8` | `VarCharVector` | |
    | `Timestamp` | `Timestamp(Micro, UTC)` | `TimeStampMicroTZVector` | |
    | `Boolean` | `Bool` | `BitVector` | |
    | `Int` | `Int(32, true)` | `IntVector` | |
    | `Long` | `Int(64, true)` | `BigIntVector` | |
    | `Short` | `Int(32, true)` | `IntVector` | Casts to Int |
    | `Byte` | `Int(32, true)` | `IntVector` | Casts to Int |
    | `Float` | `FloatingPoint(Single)` | `Float4Vector` | |
    | `Double` | `FloatingPoint(Double)` | `Float8Vector` | |
  
  ### 5.1. Vectorized Execution (Direct Binary Path)
  To achieve maximum throughput (Guideline 6), the connector implements a `PartitionReader[ColumnarBatch]` via `PubSubColumnarPartitionReader`.
  - **Zero-Copy**: Instead of converting native Arrow vectors into Spark `InternalRow`s (which entails copying data onto the JVM heap), the connector wraps the underlying Arrow memory using `ArrowColumnVector`.
  - **Memory Ownership**: The `VectorSchemaRoot` is still owned by the Reader, but its buffers are exposed directly to Spark's code-gen engine. This eliminates the "Row-by-Row" decoding overhead.
  - **Lifecycle**: `ColumnarBatch` is valid only until the next `next()` call, matching the underlying Arrow batch validity.

  ### 5.2. Observability (Custom Metrics)
  The connector reports native-side statistics to the Spark UI/History Server via `CustomTaskMetric`.
  - **`pubsub_backlog_count`**: The number of unacknowledged messages currently held in the native `ACK_RESERVOIR`. This is crucial for autoscaling and monitoring "Stuck Consumers".
  - **Implementation**: 
    - `PubSubMicroBatchStream` aggregates these metrics from `PubSubPartitionReader.getCustomMetrics()`.
    - **Status**: Currently disabled/commented out in the codebase due to compilation issues with Spark 3.3+ metric traits.
    - **Compatibility Note**: In Spark 3.3+, metrics must be imported from `org.apache.spark.sql.connector.metric`. The `CustomMetric` implementation must override `aggregateTaskMetrics` (not `aggregateMethod`), and case classes must specifically override `name()` and `value()` accessors. **Crucially**, the `CustomMetric` class (e.g., `PubSubCustomMetric`) must have a **zero-argument constructor** to allow Spark's UI reflection to instantiate it.
- **`PubSubConfig`**: Centralized registry for all configuration keys (`projectId`, `numPartitions`, `batchSize`, `lingerMs`, `maxBatchBytes`).
- **`NativeLoader`**: Thread-safe singleton for loading the native library.
  - **Automated Extraction**: To ensure portability in distributed environments (where manual `.so` installation isn't feasible), `NativeLoader` bundles the compiled Rust library in the JAR resources.
  - **Loading Strategy**:
    1.  **System Load**: Tries `System.loadLibrary` (using standard `java.library.path`).
    2.  **Fallback Extraction**: If step 1 fails, it extracts the bundled `libnative_pubsub_connector.so` from the classpath to a unique temporary directory (`spark_pubsub_native*`) and loads it using `System.load`.

## 6. Build and Version Compatibility
The connector is designed to be binary-compatible across multiple Spark versions:
- **Scala 2.12**: Required for Spark 2.4 and 3.x. Use `scala.collection.JavaConverters` for JNI interop.
- **Scala 2.13**: Required for Spark 4.0+. Use `scala.jdk.CollectionConverters`.
- **Targeting**: The project uses a multi-module build to compile against specific Spark/Scala combinations, sharing core logic via unmanaged source directories.

## 6. Logging Integration
To ensure native logs are visible in Spark's drivers and executors without needing access to stderr:
- **`NativeLogger`**: A simple Scala singleton that exposes a `log(level: Int, msg: String)` method.
- **JNI Access**: This object is accessed via JNI by a dedicated Rust background thread.
- **Safety**: The implementation catches any JNI exceptions during logging to prevent crashes in the logging subsystem itself.

## 7. JNI Error Protocol
To prevent "Silent Failures" (Spark tasks hanging indefinitely while the native background thread is dead), the connector defines a strict Error Propagation Protocol.

### 7.1. Contract
- **Negative Integers**: Used to signal fatal errors from Rust to Java.
- **Spark Obligation**: Wrappers (`PubSubPartitionReader`, `PubSubDataWriter`) **must** check for values `< 0` and throw a `RuntimeException` immediately.

### 7.2. Error Codes
| Component | Method | Code | Meaning | Action |
| :--- | :--- | :--- | :--- | :--- |
| **Reader** | `getNextBatch` | `0` | Empty (No Data) | Backoff / Yield |
| **Reader** | `getNextBatch` | `>0` | Success (1=Batch Ready) | Import Arrow Data |
| **Reader** | `getNextBatch` | `-1` | Native Ptr Invalid | Throw |
| **Reader** | `getNextBatch` | `-5` | **Channel Closed** (Background Fatal) | Throw (triggers Task Failure) |
| **Writer** | `writeBatch` | `-1` | Flush/Write Failed | Throw |
| **Writer** | `close` | `-1` | **Flush Failed** (Timeout/Error) | Throw (triggers Task Failure) |

### 7.3. Implementation Details
- **Reader**: Historically, the connector checked `result != 0` for success, which masked negative error codes as "data presence". This was fixed to strictly check `result > 0` for success. If `getNextBatch` returns `-5` (e.g., Subscription Not Found), `PubSubPartitionReader` now throws a `RuntimeException`. This ensures the Spark Scheduler sees the failure and handles retries or job abortion immediately.
- **Writer**: `NativeWriter.close()` returns an `Int`. If the final flush fails (e.g., Topic Not Found), it returns a negative code (e.g., `-1`), causing `PubSubDataWriter` to throw. This prevents "swallowing" write errors during the micro-batch commit/close phase.
