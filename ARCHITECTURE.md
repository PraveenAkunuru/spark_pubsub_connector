# Spark Pub/Sub Connector Architecture

## 1. System Overview
The Spark Pub/Sub Connector is a high-performance, native integration between Apache Spark (Structured Streaming) and Google Cloud Pub/Sub. It leverages **Apache Arrow** and **Rust** to achieve near-zero-copy data transfer and low-latency message processing.

### Key Goals
- **High Throughput**: Bypasses the JVM Pub/Sub client in favor of a native Rust `tonic` (gRPC) implementation.
- **Zero-Copy Serialization**: Uses the **Arrow C Data Interface** (FFI) to pass data between the JVM (Spark) and Native (Rust) layers without expensive serialization/deserialization steps.
- **Type Safety**: Centralized type mapping guarantees consistency between Spark SQL types and Arrow vectors.

---

## 2. High-Level Architecture
The system is composed of two primary layers:

1.  **Spark Control Plane (Scala/JVM)**:
    - Implements Spark's `DataSourceV2` API.
    - Handles query planning, partition management, and schema inference.
    - Manages the lifecycle of native resources via JNI.

2.  **Native Data Plane (Rust)**:
    - Executes heavy I/O operations (Pub/Sub Publish/Subscribe).
    - Manages gRPC connections and authentication (ADC).
    - Converts Protobuf messages to Arrow Inteface structures (`FFI_ArrowArray`, `FFI_ArrowSchema`).

```mermaid
graph TD
    subgraph Spark JVM
    Spark[Spark Executor] -->|Writes Rows| DataWriter[PubSubDataWriter]
    DataWriter -->|Buffers| ArrowVec[Arrow FieldVector]
    ArrowVec -->|Exports FFI| JNI[JNI Bridge]
    Spark -->|Columnar Reads| ColumnarReader[PubSubColumnarPartitionReader]
    Spark -->|Row Reads| RowReader[PubSubPartitionReader]
    ColumnarReader -->|Direct Arrow| JNI
    RowReader -->|Row Conversion| JNI
    end

    subgraph Native Rust
    JNI -->|Imports FFI| NativeLib[Native Writer/Reader]
    NativeLib -->|Async gRPC| PubSubClient[Google Cloud Pub/Sub]
    NativeLib -->|Off-Heap Reservoirs| AckReservoir[Ack Reservoir]
    AckReservoir -->|Deadline extension| DeadlineMgr[Deadline Manager]
    end
```

---

## 3. Core Modules

### 3.1. Read Path (Spark -> Pub/Sub)
**Classes**: `PubSubMicroBatchStream`, `PubSubPartitionReader`, `PubSubColumnarPartitionReader`
**Flow**:
1.  **Planning**: `PubSubMicroBatchStream` defines input partitions and propagates commit signals (Batch IDs) to executors.
2.  **Execution**: Readers initialize a `NativeReader` via JNI.
3.  **Fetch**:
    - `reader.getNextBatch` is called with a `batchId`.
    - Rust performs a `StreamingPull` from Pub/Sub.
    - Rust stores `ack_ids` in a **subscription-aware Native Reservoir**.
    - Rust converts `PubsubMessage`s into an `Arrow StructArray`.
    - Rust exports the array pointer via the C Data Interface.
4.  **Consumption**:
    - **Vectorized Reader**: Imports the pointer and wraps vectors in `ArrowColumnVector` for zero-copy processing in Spark.
    - **Standard Reader**: Iterates over Arrow increments and creates Spark `InternalRow`s.
5.  **Signal Propagation**:
    - On batch commit, Spark sends a batch ID signal.
    - In the next planning cycle, this signal is passed to all executor tasks.
    - Executors call `ackCommitted`, which flushes `ack_ids` from the Native Reservoir for the committed batches.

### 3.2. Write Path (Pub/Sub -> Spark)
**Class**: `PubSubDataWriter`
**Flow**:
1.  **Buffering**: Rows are written to `PubSubDataWriter`, which buffers them into local Arrow `FieldVector`s using `ArrowUtils.setValue`.
2.  **Flush**: When the buffer is full (configured batch size), `writeBatch` is invoked.
3.  **Export**:
    - Scala exports the local Arrow Vector to a C-compatible struct (`FFI_ArrowArray`).
    - **Critical**: Scala calls `close()` on the local root, while Rust takes a reference.
4.  **Publish**:
    - Rust imports the FFI struct via **transmuted Mirrors** (bypassing `arrow-rs` strict pointer layouts for compatibility).
    - Rust converts Arrow batches to `PubsubMessage`s via `ArrowBatchReader`.
    - Messages are published asynchronously via an internal MPSC channel in `PublisherClient`.
    - Panic protection (`catch_unwind`) ensures JNI errors don't crash the JVM.

---

## 4. Key Process Flows

### 4.1. FFI Ownership Model (Reference Counting)
We discovered that Java's Arrow implementation uses **Reference Counting**, not strict "Move" semantics (as in C++ `std::move`).

- **Incorrect Approach**: Zeroing out the source pointer triggers a JVM crash/panic because the Java side still expects the struct to exist for its own metadata.
- **Correct Approach**:
    - **Java**: Allocate -> Export -> Call `close()` (Decrements local ref).
    - **Rust**: Import (Increments ref) -> Process -> Drop (Decrements ref).
    - **Result**: Memory is freed when the count reaches zero.

### 4.2. Centralized Type Mapping (`ArrowUtils.scala`)
All type conversions are centralized to ensure consistency.
- **Support**: `Binary`, `String`, `Timestamp`, `Boolean`, `Int`, `Long`, `Float`, `Double`, `Short`, `Byte`.
- **Logic**: Maps Spark `DataType` to specific Arrow `FieldVector` implementations (e.g., `StringType` -> `VarCharVector`).

---

## 5. Build Structure (Version Focused)
The project identifies compatible Spark versions via dedicated sub-modules:

- **`spark33`**: Targets Spark 3.3.4 (Scala 2.12).
- **`spark35`**: Targets Spark 3.5.0 (Scala 2.12) - Primary Target.
- **`spark40`**: Targets Spark 4.0.0-preview2 (Scala 2.13).

All modules share the core implementation in `src/main/scala` via `unmanagedSourceDirectories`.

---

## 6. Native Library (`native/`)
- **Crate**: `native_pubsub_connector`
- **Dependencies**: `arrow`, `tonic`, `tokio`, `google-cloud-pubsub`.
- **Safety**: Uses `unsafe` blocks only for FFI bridges, documented with safety invariants.
