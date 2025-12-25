# Centralized JNI Logging

## Overview
The Centralized JNI Logging system bridges Rust native logs (generated via the `log` crate) to Spark's standard JVM logging system (SLF4j/Log4j). This ensures that critical native events—such as connection failures, authentication issues, and panics—are visible in the main Spark executor logs (`stderr/stdout`) without requiring separate access to container streams.

## Architecture

The system uses an **Asynchronous Bridge** pattern to decouple the high-performance Rust data plane from the JNI usage, preventing strictly JNI-related overhead or blocking from affecting throughput.

### Data Flow
1.  **Origin**: Rust code calls `log::info!`, `log::error!`, etc.
2.  **Capture**: Custom `JniLogger` struct (implementing `log::Log`) captures the record.
3.  **Buffer**: Record is sent to a **bounded** `mpsc` channel (capacity 10,000).
4.  **Process**: A dedicated background thread:
    *   Reads from the channel.
    *   Attaches to the JVM (if not already attached).
    *   Invokes the static Scala method `NativeLogger.log`.
5.  **Output**: Scala delegates to `org.slf4j.Logger`, which writes to the Spark executor log.

## Components

### 1. Rust Side (`native/src/logging.rs`)
*   **`init(vm: JavaVM)`**: Entry point. Spawns the background thread and sets up the global logger with a **bounded channel** (size 10,000) to prevent memory leaks if the JNI thread stalls.
*   **`JniLogger`**: Lightweight implementation of `log::Log`. Uses `try_send` to drop logs if the channel is full, ensuring the hot path never blocks or OOMs due to logging.
*   **Background Thread**: 
    *   Uses `vm.attach_current_thread_permanently()` to ensure it can make JNI calls.
    *   Resolves `com/google/cloud/spark/pubsub/NativeLogger$` using `AutoLocal` to manage JNI references safely.
    *   Maps Rust `Level` enum to integer codes for the JVM.

### 2. Scala Side (`NativeLogger.scala`)
*   **`com.google.cloud.spark.pubsub.NativeLogger`**: A singleton `object`.
*   **`def log(level: Int, msg: String)`**:
    *   Decodes level (1=Error, 2=Warn, 3=Info, 4=Debug, 5=Trace).
    *   Calls appropriate SLF4j method (e.g., `logger.error(msg)`).

### 3. Integration Points (`lib.rs`)
*   **Initialization**: `init` is called once during `NativeReader.init`.
*   **Panic Handling**: `std::panic::catch_unwind` blocks in JNI exports verify execution safety; if a panic occurs, `log::error!` allows the panic message to be visible in Spark logs before the native function returns a failure code.

## Key Design Decisions

### Why Asynchronous?
JNI calls have overhead. Calling back into the JVM from the hot data path (processing thousands of messages/sec) would degrade performance. The channel allows the data plane to "fire and forget" logs.

### Thread Safety
*   **JNIEnv**: Not thread-safe. The background thread obtains its own `JNIEnv` by attaching to the VM.
*   **Class Lookup**: JNI class lookup can fail in system threads. We resolve `NativeLogger` once and keep a global reference (or resolving it within the attached thread context).

### Level Mapping
| Rust Level | Code | SLF4j Level |
|------------|------|-------------|
| Error      | 1    | Error       |
| Warn       | 2    | Warn        |
| Info       | 3    | Info        |
| Debug      | 4    | Debug       |
| Trace      | 5    | Trace       |

## Usage vs. `eprintln!`
*   **`eprintln!`**: Direct write to stderr. Good for fatal crashes if logging fails, but often gets lost or interleaved poorly in Spark.
*   **`log::*`**: Structured, leveled, and routed through Spark's layout configurations. Prefer `log::*` for all persistent messaging.
