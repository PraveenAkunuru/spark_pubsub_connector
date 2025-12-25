# Stability Hardening Implementation Patterns

This document details the specific code patterns and logic implemented to satisfy the [Stability and Scale Design Guidelines](../architecture/stability_and_scale_guidelines.md).

## 1. JNI Panic Barriers (Rust)
**Problem**: A panic in Rust code (e.g., `unwrap()` on a `None`) unwinds the stack across the FFI boundary, causing the entire JVM process to abort (SIGABRT/SIGSEGV).
**Solution**: Wrap all JNI entry points in `std::panic::catch_unwind`.

### Pattern
```rust
pub extern "jni" fn nativeMethod(self, _env: &JNIEnv, ...) -> i32 {
    let result = std::panic::catch_unwind(|| {
        // ... business logic ...
        // ... potentially panic-prone code ...
        1 // Success code
    });

    match result {
        Ok(res) => res,
        Err(_) => {
            eprintln!("Rust: Panic occurred in nativeMethod");
            -100 // Specialized error code for Panics
        }
    }
}
```
**Operational Note**: If Spark logs show an operation returning `-100`, it indicates a suppressed Rust panic. Check `stderr` or executor logs for the "Rust: Panic occurred" message.

### Critical Methods to Protect
1.  **`init`**: Prevents Spark task failures from crashing the whole executor during startup.
2.  **`close`**: **Crucial**. If `close` panics (e.g., during double-free checks), it must be caught to ensure the JVM can still release the TaskContext and not SIGABRT. A crash in `close` often masks the original error.
3.  **Data Methods** (`getNextBatch`, `writeBatch`): Protects the main data loop.

## 2. Initialization Jitter (Rust)
**Problem**: When 1000+ executors start simultaneously, they all hit the Pub/Sub API's `StreamingPull` endpoint at the exact same millisecond, causing `RESOURCE_EXHAUSTED` or quota throttling.
**Solution**: Introduce a random sleep (Jitter) before the heavy lifting in `init`.

### Implementation
```rust
// In lib.rs :: init
let mut rng = rand::thread_rng();
// 0-500ms jitter is usually sufficient to spread the spike
let delay_ms = rand::Rng::gen_range(&mut rng, 0..500);
std::thread::sleep(std::time::Duration::from_millis(delay_ms));
```

## 3. Partition Clamping (Scala)
**Problem**: Users may blindly set `unbounded` partitioning or extremely high numbers (e.g., 20,000 partitions) which exceeds the Google Cloud Pub/Sub quota for `active_streaming_pull_connections` (limit 10,000 per project).
**Solution**: Enforce a hard cap on `numPartitions` relative to expected resources, typically 2x the executor cores (default parallelism).

### Logic (`PubSubMicroBatchStream.scala`)
```scala
val defaultParallelism = SparkSession.active.sparkContext.defaultParallelism
val maxPartitions = defaultParallelism * 2
val requestedPartitions = options.getOrElse(..., ...).toInt

val numPartitions = if (requestedPartitions > maxPartitions) {
  logWarning(s"Reducing numPartitions from $requestedPartitions to $maxPartitions...")
  maxPartitions
} else {
  requestedPartitions
}
```

## 4. Reservoir Observability
**Problem**: The "Off-Heap Reservoir" is opaque. Operators cannot tell if it is growing boundlessly (memory leak risk).
**Solution**: Expose a `getUnackedCount` JNI method that locks the reservoir and sums the vector sizes.

### Usage
- **Spark Metric**: This value should ideally be reported as a custom metric `num_unacked_messages`.
- **Debugging**: Can be printed in logs during commits to verify the reservoir empties as expected.

## 5. Robust Bidirectional Streaming (Rust)
**Problem**: Rust's `StreamingPull` is bidirectional (receiving messages vs. sending ACKs). It requires managing two async streams simultaneously while handling connection failures and backoff, without one blocking the other.
**Solution**: Use a `tokio::select!` loop inside a dedicated background task that owns the gRPC connection.

### Pattern (`pubsub.rs`)
```rust
loop {
    // 1. Re-establish connection with Backoff
    if let Err(e) = connect().await {
        sleep(backoff).await;
        backoff = min(backoff * 2, max_backoff);
        continue;
    }

    // 2. Bidirectional Select Loop
    loop {
        tokio::select! {
            // A: Incoming Messages from Pub/Sub
            msg_res = g_rpc_stream.message() => {
                match msg_res {
                    Ok(Some(msg)) => internal_tx.send(msg).await?,
                    _ => break, // Stream closed/error -> outer loop reconnects
                }
            }
            // B: Outgoing Requests (Acks) from Spark
            req_opt = external_rx.recv() => {
                match req_opt {
                    Some(req) => g_rpc_tx.send(req).await?,
                    None => return, // External channel closed -> shutdown task
                }
            }
        }
    }
}
```

## 6. FFI Safety: Ownership Transfer (Rust)
**Problem**: The Apache Arrow C Data Interface allows passing ownership of Arrow arrays between runtimes (Java -> Rust). A naive implementation might zero out the source C structs *before* the receiver has fully imported them, or worse, both sides might try to free the same memory, leading to double-free corruption or segfaults.
**Solution**: Use the "Move" semantic correctly. When exporting from Java to Rust via `ArrowArray` and `ArrowSchema` pointers:
1.  **Java Side**: Allocate structs, export data, pass pointers to Rust, and *then close* the local Java `VectorSchemaRoot`. Java's allocator decrements its refcount, but because it was exported, the underlying memory remains valid until the *importer* frees it.
2.  **Rust Side**: Read the pointers, validate them, and import using `ArrowArray::from_ffi`.
3.  **Critical Rule**: **Do NOT zero out** the source pointers in Rust if you are just importing. The `ArrowArray::from_ffi` takes ownership of the release callback. If you zero it out manually, you break the contract and might leak memory or crash if Java tries to clean up an empty struct.

### Pattern (`lib.rs`)
```rust
// CORRECT:
let array_val_final: FFI_ArrowArray = std::mem::transmute(array_mirror);
let schema_val_final: FFI_ArrowSchema = std::mem::transmute(schema_mirror);

// Import takes ownership of the release callback
let array_data = arrow::ffi::from_ffi(array_val_final, &schema_val_final)?;

// The memory will be freed when `array_data` is dropped in Rust.
// Java's GC/close() handles the Java-side metadata, but the data buffer lifecycle is now Rust's responsibility.

// CRITICAL: Manual Release on Error
// If proper import fails (e.g., from_ffi returns Err), Rust still "owns" the pointers but hasn't created the wrapper 
// that drops them. You MUST manually invoke the release callback to prevent leaks.
if let Err(e) = import_result {
    // Manually cast back to mirror and call release
    let sm = &*(schema_ptr as *const FfiArrowSchemaMirror);
    if let Some(release) = sm.release {
        release(schema_ptr);
    }
    return Err(e);
}
```

## 7. Sink Flow Control (Smart Flushing)
**Problem**: Relying solely on `batchSize` (row count) can cause OOMs or extremely large payloads if rows are unexpectedly wide (e.g., serialized blobs), while relying solely on `lingerMs` can lead to poor throughput for high-velocity streams.
**Solution**: Implement a hybrid thresholding strategy that triggers a flush on *whichever* limit is hit first: Row Count, Byte Size, or Time.

### Logic (`PubSubWrite.scala`)
```scala
// Triple-check flush condition
if (rowCount >= batchSize || 
    currentBatchBytes >= maxBatchBytes || 
    (now - lastFlushTime >= lingerMs)) {
  flush()
}
```
**Configuration**:
- `batchSize`: Default 1000 rows.
- `maxBatchBytes`: Default 5MB (aligned with Pub/Sub's preferred request size).
- `lingerMs`: Default 1000ms.

## 8. Synchronous Sink Flush (The "Async Gap" Fix)
**Problem**: Async publishing is great for throughput, but if the Spark task finishes and the executor dies immediately after `writeBatch` returns, queued messages in the background channel are lost.
**Solution**: Implement a `Flush` command in the sequence of operations. The `close()` method must block until the background publisher confirms all preceding messages are sent.

### Pattern (`pubsub.rs`)
```rust
enum WriterCommand {
    Publish(Vec<PubsubMessage>),
    Flush(tokio::sync::oneshot::Sender<()>), // Carries the return signal
}

// In background loop:
match cmd {
    WriterCommand::Publish(msgs) => client.publish(msgs).await,
    WriterCommand::Flush(ack_tx) => {
        // Because the channel is ordered, reaching here means all previous publishes are done.
        let _ = ack_tx.send(()); 
    }
}
```
**Verification**: Verify "Flush completed" logs appear at the end of every task.

## 9. Lifecycle-Bound Background Tasks
**Problem**: In distributed environments like Spark, tasks (and their native runtimes) are created and destroyed frequently. A `static` guard (e.g., `static STARTED: AtomicBool`) intended to prevent duplicate background tasks can accidentally prevent *any* task from running if the original runtime dies but the static flag remains true. This leads to silent failures of maintenance tasks (like Ack Deadline Extension), resulting in memory leaks or infinite redelivery.
**Solution**: 
1.  **Avoid Static State**: Do not use `static` guards for tasks that depend on a specific lifecycle (like a Tokio Runtime).
2.  **Fate Sharing**: Spawn background tasks unconditionally for each new instance of the resource they manage. Let the runtime's drop mechanics handle the cleanup (task cancellation).

### Pattern (`pubsub.rs`)
```rust
// INCORRECT (Static Guard):
pub fn start_manager(rt: &Runtime) {
    static STARTED: AtomicBool = AtomicBool::new(false);
    if !STARTED.swap(true, Ordering::SeqCst) {
        rt.spawn(async { ... }); // Re-spawn prevented even if previous RT died!
    }
}

// CORRECT (Unconditional Spawn):
pub fn start_manager(rt: &Runtime) {
    rt.spawn(async { ... }); // New task for every new Runtime.
                             // When 'rt' is dropped, this task is cancelled automatically.
}
```
**Verification**: Ensure memory usage is stable over long-running tests with multiple partitions/tasks.

## 10. Robust Publisher Backoff
**Problem**: The Write path (Publish API) is subject to transient failures (transport errors, "Service not ready"), especially when running against the local Emulator or under high load. Failing immediately causes data loss or job abatement.
**Solution**: Implement an explicit retry loop with exponential backoff around the gRPC `publish` call. Clamping the backoff prevents infinite stalls, while the loop ensures temporary blips don't crash the task.

### Pattern (`pubsub.rs`)
```rust
let mut backoff_millis = 100;
let max_backoff = 60000;
loop {
    let request = Request::new(req.clone());
    match client.publish(request).await {
        Ok(_) => break, // Success
        Err(e) => {
            eprintln!("Publish failed: {:?}. Retrying in {}ms", e, backoff_millis);
            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_millis)).await;
            backoff_millis = std::cmp::min(backoff_millis * 2, max_backoff);
        }
    }
}
```

## 11. Safe Write Path FFI (Import-Only)
**Problem**: When Spark exports data to Rust (Write Path), it uses `exportVectorSchemaRoot`. If Rust attempts to "zero out" the pointers in the Java memory space (thinking it needs to enforce a "move"), it corrupts the JVM's reference counting mechanism, leading to `SIGSEGV` or `double free`.
**Solution**: 
1.  **Import**: Use `ArrowArray::from_ffi` to take ownership of the *reference* (via the release callback).
2.  **No Touch**: Do **not** modify the source C structs (the pointers passed as `jlong`).
3.  **Drop**: Rely on Rust's `Drop` implementation to call the release callback when processing is done.

### Pattern (`lib.rs`)
```rust
// 1. Read pointers
let array = unsafe { std::ptr::read(array_ptr as *const FFI_ArrowArray) };
let schema = unsafe { std::ptr::read(schema_ptr as *const FFI_ArrowSchema) };

// 2. Import (Rust takes ownership of this specific reference)
let record_batch = arrow::ffi::import_array_from_c(array, &schema)?;

// 3. Process
process_batch(&record_batch).await?;

// 4. Cleanup (Automatic)
// record_batch is dropped -> calls release callback -> Java decrements refcount.
```

## 12. Hot Path Log Hygiene
**Problem**: Logging high-frequency events (e.g., "Received message ID X") in the hot data loop causes severe performance degradation (I/O saturation) and log flooding, making it impossible to find real errors.
**Solution**: Remove or strictly sample logging in tight loops (`next()`, `writeBatch`, message receipt).
*   **Anti-Pattern**: `eprintln!("Received {}", msg.id);` inside a loop processing 10k msg/sec.
*   **Correct**: Log only on state changes (Connected/Disconnected), errors, or periodic summaries (e.g., "Processed 10,000 messages").



## 13. Bounded Diagnostic Channels
**Problem**: Auxiliary systems like Logging or Metrics often use asynchronous channels to decouple from the hot path. If the consumer (e.g., JNI thread) stalls or is slower than the producer (Rust data plane), an unbounded channel will grow indefinitely, causing an OOM (Out of Memory) crash.
**Solution**: Always use **Bounded Channels** for diagnostics. If the channel is full, drop the diagnostic event rather than crashing the application.

### Pattern (`logging.rs`)
```rust
// 1. Use Bounded Channel (e.g., 10,000 items)
let (tx, mut rx) = mpsc::channel::<(Level, String)>(10000);

// 2. Producer: Try Send (Non-blocking)
if let Some(tx) = &SENDER {
    // If full, we silently drop the log. Better to lose a log than crash the executor.
    let _ = tx.try_send((record.level(), record.args().to_string()));
}
```

## 14. Synchronous Resource Validation (Fail Fast)
**Problem**: Asynchronous initialization (e.g., spawning a background task immediately) can mask configuration errors like invalid subscription names or missing permissions. Use cases fail silently or hang indefinitely while the background task retries forever.
**Solution**: Perform a blocking (or awaited) resource check *before* spawning the background task. If the check fails, return an error immediately to variables the caller (Spark) to fail the task.

### Pattern (`pubsub.rs`)
```rust
// 1. Validate Synchronously
let check_req = GetSubscriptionRequest { subscription: full_name.clone() };
match client.get_subscription(Request::new(check_req)).await {
    Ok(_) => log::info!("Subscription validated"),
    Err(e) => return Err(Box::new(e)), // Fail Fast
}

// 2. Only then Spawn Background Task
tokio::spawn(async move { ... });
```

## 15. Fatal Error Discrimination
**Problem**: Infinite retry loops are robust for transient network issues but disastrous for permanent errors (e.g., `NotFound`, `PermissionDenied`). A "zombie" background task will retry forever, causing the Spark task to hang until timeout.
**Solution**: Inspect the gRPC/Status error code. If it indicates a permanent failure, break the retry loop and signal a fatal error or exit the task to trigger a failure downstream.

### Pattern (`pubsub.rs`)
```rust
Err(e) => {
    let code = e.code();
    if code == tonic::Code::NotFound || code == tonic::Code::PermissionDenied {
        log::error!("Fatal Error: {:?}. Stopping.", e);
        return; // Break loop, stop task
    }
    // Continue retrying for transient errors (Unavailable, etc.)
}
```

## 16. Backpressure-Aware Ingestion Loop
**Problem**: If the internal channel to Spark fills up (backpressure), a naive `recv()` loop from gRPC might get blocked trying to `send()`. If the gRPC stream requires bidirectional communication (e.g., sending Acks), blocking the loop prevents Acks from being processed, leading to redelivery storms.
**Solution**: Use a robust `select!` loop that prioritizes external signals (Acks) and only pulls from the data stream if there is capacity ("Backpressure Guard").

### Pattern (`pubsub.rs`)
```rust
loop {
    let internal_buf_full = pending_msgs.len() >= limit;
    tokio::select! {
        // 1. Always process Acks (High Priority)
        ack = ack_rx.recv() => handle_ack(ack).await,
        
        // 2. Only pull data if we have space (Backpressure)
        msg_res = stream.message(), if !internal_buf_full => {
             buffer.push(msg_res?);
        }
    }
}
```

