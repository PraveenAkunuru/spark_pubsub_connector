# Rust Data Plane: Details & Patterns

This document provides a technical deep dive into the Rust implementation of the Spark Pub/Sub connector.

## 1. Architectural Overview
The Rust layer is responsible for the performance-critical path:
- **gRPC Ingestion & Publishing**: Uses `tonic` and `tokio` for high-throughput `StreamingPull` and `Publish`.
- **Arrow Conversion**: Columnar transformation of Pub/Sub messages.
- **FFI Memory Management**: Zero-copy handoff to/from the JVM via the Apache Arrow C Data Interface.
- **Safety**: Comprehensive panic handling (`catch_unwind`) and memory safety checks on both Read and Write paths.

## 2. Pub/Sub gRPC Orchestration
The `PubSubClient` (in `pubsub.rs`) manages a background task for `StreamingPull`.
- **Async Bridging**: Spark's executor threads are synchronous. Rust uses a bounded MPSC channel (1000 messages) as a buffer. The `StreamingPull` task pushes to this channel, and JNI `fetch_batch` polls it.
- **Synchronous Validation**: `PubSubClient::new` performs a blocking `get_subscription` call to fail fast if the subscription does not exist or permissions are missing.
- **Reliable Acknowledgments**: The background task maintains a bidirectional stream for acknowledgments. A `Sender` channel allows foreground JNI calls to queue `ack_id`s for asynchronous transmission.
- **Backpressure Prioritization**: To prevent acknowledgments from being "starved" during high-throughput data ingestion, the background `StreamingPull` task uses `tokio::select!` to prioritize processing the `ext_rx` channel (containing outgoing Acks/Modacks) over pulling new messages from the gRPC stream. This ensures that even when the local 1000-message buffer is full, the system can still process control signals to prevent lease expiration.
- **Context Awareness**: Stores the full `subscription_name` upon instantiation for precise deadline extension in the global `ACK_RESERVOIR`.
- **Error Propagation**: Fatal errors in the background task cause the internal channel to close. JNI `fetch_batch` returns `-5`, triggering a Spark task failure.

### 2.1. Sink Optimization: Async Publishing (Implemented Phase 10)
To maximize throughput in the write path, the native publisher uses a background task model:
- **Non-blocking `write`**: Spark's `DataWriter` hands off batches to a Rust `tokio::sync::mpsc` channel.
- **Background Publisher**: A dedicated Tokio task uses a `WriterCommand` enum to handle both `Publish` payloads and `Flush` signals.
- **Latency Decoupling**: Spark returns from the JNI `writeBatch` call as soon as the data is queued.
- **Synchronous Close**: To prevent data loss (The "Async Gap"), `NativeWriter.close()` sends a `WriterCommand::Flush` and awaits a `oneshot` response, ensuring all queued messages are pushed to the Pub/Sub client before the task exits.
- **Error Handling**: Failures in the background task are logged to `eprintln!`. Permanent errors (e.g., `NotFound`) trigger a fatal stop to prevent infinite retries.

### 2.2. Multi-Task Resource Optimization (Connection Pooling)
To reduce the overhead of establishing TLS connections when multiple Spark tasks run on the same executor:
- **`CONNECTION_POOL`**: A static, thread-safe `Lazy<Mutex<HashMap<String, Channel>>>` in `pubsub.rs`.
- **Channel Reuse**: Channels are indexed by their endpoint (Host/Port). New `NativeReader` or `NativeWriter` instances first check the pool before connecting.
- **Benefits**: Significant reduction in Executor CPU usage and TCP socket churn.
- **Async Safety**: Crucially, the implementation ensures that the `MutexGuard` for the `CONNECTION_POOL` is dropped **before** any `await` calls (e.g., during `connect()`). This prevents deadlocks and "Mutex held across await" errors in the Tokio runtime.

## 3. Arrow C Data Interface (FFI)
Zero-copy transfer is achieved via the Apache Arrow C Data Interface.

### 3.1. Ownership Patterns
To ensure stability across JDK versions (17, 21, 25), two primary ownership patterns are used:

#### Read Path: Standard Move (Rust -> Scala)
Used in `NativeReader.getNextBatch`. Rust produces data, transfers ownership to Java.
1.  **Allocation**: Rust creates `FFI_ArrowArray` and `FFI_ArrowSchema`.
2.  **Move**: Rust uses `std::ptr::write(out_ptr, val)` to move the structs into memory provided by Scala.
3.  **Consumption**: The data now lives at the C pointer address. Rust's local variable is consumed, and its destructor (which would call `release`) is bypassed. Java takes ownership via `importVectorSchemaRoot`.

#### Write Path: Balanced Release (Scala -> Rust)
Used in `NativeWriter.writeBatch`. Rust imports the data without corrupting Java's view.
1.  **Validation**: Rust reads the FFI structs and uses **Mirror Structs** (ABI-aligned public replicas) to perform deep validation.
2.  **Move**: Rust reads the structs via `std::ptr::read` but **DOES NOT zero out** the source pointers.
3.  **Result**: 
    - **Success**: Rust takes ownership via `ArrowArray::from_ffi`. The memory is freed when Rust drops the imported struct.
    - **Failure**: If validation or `from_ffi` fails, Rust **must manually invoke the release callback** on the raw FFI pointers to prevent memory leaks, as the explicit ownership transfer didn't complete.

### 3.2. Mirror Pattern
Used to inspect or modify private FFI fields (like `release`) before passing them to the `arrow-rs` library.
```rust
#[repr(C)]
pub struct FfiArrowSchemaMirror {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut FfiArrowSchemaMirror, // Recursive pointer type might differ in impl
    pub dictionary: *mut FfiArrowSchemaMirror,
    pub release: Option<unsafe extern "C" fn(*mut FfiArrowSchemaMirror)>,
    pub private_data: *mut c_void,
}
```

## 4. JNI Infrastructure
- **`robusta_jni`**: Used for lightweight JNI bridging.
- **Persistence**: A `Box<RuntimeState>` is converted to a `jlong` (raw pointer) and stored in the Scala object to maintain state across calls.
- **Lint Suppression**: The JNI module uses `#[allow(clippy::all)]` to suppress boilerplate warnings from macro-generated code.
- **Native Loading**: Loading is triggered via `NativeLoader` in Scala, which manages `System.loadLibrary` as a thread-safe singleton.

## 5. Reliability: Off-Heap Ack Reservoir
The native layer implements an off-heap storage mechanism to handle acknowledgments without saturating the JVM heap.
- **`ACK_RESERVOIR`**: A global static `Lazy<Mutex<AckReservoirMap>>`.
- **Type Aliasing**: Uses `type AckReservoirMap = HashMap<String, HashMap<String, Vec<String>>>` to satisfy clippy's `type-complexity` lint and improve readability of the global state.
- **Integrated Storage**: When a Spark task calls `getNextBatch`, the native layer captures all `ack_id`s from the fetched messages and stores them in the `ACK_RESERVOIR` *before* the batch is converted to Arrow and exported. This avoids a common FFI pitfall where subsequent JNI calls attempting to re-process the same Arrow memory can trigger double-frees or segmentation faults.
- **Signal-Based Flush**: When a "Commit Signal" is received from the Driver (via `ackCommitted`), the Rust layer retrieves the IDs for the committed batches and executes the gRPC `acknowledge` call asynchronously.
- **Wait-Free Operation**: The JNI calls for storing and flushing acks are non-blocking, ensuring that the Spark control loop is never delayed by network I/O with Pub/Sub.
- **Deadline Management**: Implemented via a background task (`start_deadline_manager`) which is spawned during **each** `NativeReader::init`.
    - **Fix for Memory Leak**: Previous implementation used a `static STARTED` guard which prevented the manager from restarting on new `NativeReader` instances, causing the `ACK_RESERVOIR` to grow indefinitely as messages were never extended or cleaned up.
    - **Fate Sharing**: The new design ensures the manager's lifecycle is tied to the Reader's Runtime, automatically cleaning up when the reader stops. It runs periodically (e.g., every 5 seconds) to extend deadlines for uncommitted batches.

## 6. Code Hardening & Linting Standards

To maintain the high-performance native bridge, the project enforces strict linting via `cargo clippy`:
- **Digit Grouping**: Large numeric literals (especially microsecond timestamps) must use consistent underscore grouping (e.g., `1_600_000_000_000_000`) to prevent off-by-one errors in constants.
- **Panic Protection**: All JNI-exported functions are wrapped in `std::panic::catch_unwind` to prevent Rust panics from unwinding into the JVM, which would cause an unrecoverable crash.
- **Complexity Management**: Deeply nested types (like the acknowledgment reservoir) are factored into type aliases to improve maintainability and satisfy static analysis tools.

## 7. NativeWriter Implementation
The `NativeWriter` struct (exposed via JNI) encapsulates the write path logic.
- **FFI Signature**: 
    - `init(project, topic) -> jlong`: Initializes a dedicated `Tokio` runtime and `PublisherClient`, returning a raw pointer to the heap-allocated `RustPartitionWriter`.
    - `writeBatch(ptr, arrow_array_addr, arrow_schema_addr) -> i32`: Accepts Arrow FFI pointers, verifies schema, and queues messages.
- **Hardening Features**:
    - **Panic Safety**: `writeBatch` and `close` are wrapped in `std::panic::catch_unwind`.
    - **Memory Safety**: Strictly follows Arrow C Data Interface ownership rules. If `from_ffi` fails, it manually releases the FFI pointers to prevent leaks.
    - **Graceful Shutdown**: `close()` explicitly calls `flush()` with a 30-second timeout. If the background task is dead or unresponsive, it logs an error and proceeds to shutdown, preventing indefinite hangs.

## 8. Centralized Logging (JNI Log Bridge)
To provide a unified debugging experience, the connector bridges Rust's `log` crate records directly into Spark's `Log4j` system.

### 8.1. Architecture
- **Rust Side (`logging.rs`)**:
    - Implements a custom `log::Log` trait.
    - Spawns a dedicated background thread initialized with `JavaVM::attach_current_thread_permanently`.
    - Uses a **bounded** MPSC channel (10,000 capacity) to decouple high-performance threads from JNI transitions, preventing memory leaks if the consumer stalls.
    - **Optimization**: Hot-path logging checks (`log::enabled`) are practically free (atomic check), while actual JNI calls happen asynchronously.
- **Scala Side (`NativeLogger.scala`)**:
    - Singleton object `com.google.cloud.spark.pubsub.NativeLogger`.
    - Bridges integer log levels (1=Error, ..., 5=Trace) to `org.slf4j.LoggerFactory`.
- **Flow**: `log::info!` -> Channel -> Background Thread -> JNI Call -> `NativeLogger.log()` -> Slf4j -> Log4j Appender.

### 8.2. Implementation Nuances
- **Detailed JNI Class Lookup**: When resolving classes on a background thread (e.g., via `env.find_class`), the resulting `JClass` object must be wrapped in an `AutoLocal` (or `GlobalRef`) to satisfy the `Desc<'_, JClass<'_>>` trait bound required for subsequent JNI calls (like `get_static_field_id`). **Caution**: `AutoLocal::new` (in `jni 0.19`) expects the environment first: `AutoLocal::new(&env, obj)`.

## 9. Native Schema Projection (Implemented Phase 2)
The data plane supports offloading payload parsing to Rust via the `ArrowBatchBuilder`'s structured mode.

### 9.1. Dual-Mode Operation
- `ArrowBatchBuilder` initializes either a `payloads: BinaryBuilder` (Raw Mode) or a `struct_builder: StructBuilder` (Structured Mode).
- **Mode Selection**: Decided at instantiation time. If a `SchemaRef` is passed from JNI, the builder enters structured mode.

### 9.2. JSON-to-Arrow Mapping Logic
To handle high-performance parsing of JSON payloads, the connector uses a recursive visitor pattern:
1. **Parser**: Uses `serde_json::from_slice` to deserialize the raw Pub/Sub `data` into a `Value`.
2. **Metadata Filtering**: The builder explicitly filters out reserved metadata fields (`message_id`, `publish_time`, `ack_id`, `attributes`) from the provided schema. This ensures that the `StructBuilder` only manages user-defined data payload fields, preventing structural collisions with the connector's internal metadata columns.
3. **Field Alignment**: The builder iterates over the fields of the internal `struct_fields`. For each field, it performs a keyed lookup in the JSON object.
4. **Type Coercion**:
   - `DataType::Utf8`: Converts scalars to strings or extracts string values.
   - `DataType::Int32` / `Int64`: Extracts `as_i64` and casts accordingly.
   - `DataType::Float64`: Extracts `as_f64`.
   - `DataType::Boolean`: Extracts `as_bool`.
   - `DataType::Struct`: Recursively calls `append_json_to_struct` on nested JSON objects.
5. **Resilience**: If a field is missing in JSON or a type mismatch occurs, an explicit `append_null()` (or `append(false)` for nested structs) is called to maintain column length alignment across the batch.

### 9.3. Implemented Refactor: Dynamic Builder Vector
During implementation, it was found that the standard `arrow-rs` `StructBuilder` does not always expose mutable child builders generically (`element_builder` might be private or version-dependent in Spark's target Arrow versions).

**Solution**: The `ArrowBatchBuilder` was refactored to use a `Vec<Box<dyn ArrayBuilder>>` stored alongside a `Vec<Field>`.
- **Dynamic Mapping**: The recursive visitor pass now looks up the appropriate builder by index (`builders[i]`) and uses `as_any_mut().downcast_mut::<T>()` for type-safe appends.
- **Robustness**: This approach provides maximum control over field alignment and simplifies cases where a field might be missing in a specific JSON record (facilitating explicit `append_null` calls).
- **Flattening**: In `finish()`, each builder is finalized into an `ArrayRef`. These are combined with the metadata arrays into a single flat list, matching Spark's expected schema layout.

### 9.4. Memory Management
The `StructBuilder` (or the underlying array builders) internally manages the memory for all child arrays. When `finish()` is called, the resulting arrays are finalized and exported to FFI.

## Appendix A: Off-Heap Signal-Based Acknowledgment (Deep Dive)

### A.1. Problem Context
Directly managing `ack_ids` in Spark creates massive GC pressure (millions of string objects) and risks FFI double-free errors. Moreover, acknowledging too early causes data loss, while too late causes redelivery spikes.

### A.2. Architecture
1.  **Native Reservoir**: A global `Lazy<Mutex<AckReservoirMap>>` in Rust stores `ack_ids` immediately upon fetch.
2.  **Signal Propagation**: Spark Driver piggybacks "Commit Signals" on `PlanInputPartitions`. Executors receive these signals and call `ackCommitted` via JNI.
3.  **Background Deadline Manager**: A dedicated Tokio task periodically scans the reservoir and extends deadlines for uncommitted messages, preventing prematurely redelivery without blocking the main ingest path.

### A.3. Key Fixes
-   **Memory Leak Fix (Fix B)**: Refactored `start_deadline_manager` to spawn per-runtime tasks instead of relying on a static `STARTED` flag, preventing indefinite accumulation in the reservoir.
-   **Zero-Copy Capture**: Ack IDs are extracted *before* Arrow export to avoid FFI safety issues.

### A.4. Verification
Verified via `AckIntegrationTest`: Messages are consumed, committed, and NOT redelivered upon restart.
