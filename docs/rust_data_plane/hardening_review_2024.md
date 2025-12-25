# Independent Reliability & Hardening Review (December 2024)

## Overview
This document captures critical findings from an independent code audit conducted in December 2024. The audit focused on "Async Gaps" in data persistence, FFI memory safety, and long-running scalability. These findings form the basis for the Phase 12 stability hardening effort.

## Critical Findings

### 1. Sink Reliability: The "Async Gap"
*   **Risk**: Data Loss
*   **Severity**: Critical
*   **Description**: The `NativeWriter.writeBatch` method returns control to the Spark ecosystem immediately after placing messages in a Tokio channel. This allows Spark to consider a task "successful" before the data is actually persisted to Pub/Sub. If the executor crashes in this window (after `writeBatch` returns but before the background task publishes), data is lost despite "At-Least-Once" expectations.
*   **Required Fix (Pattern)**: Implement a **Synchronous Flush**. The `NativeWriter.close()` method must block until a signal is received from the background Tokio task confirming that all pending messages have been passed to the underlying Pub/Sub client.

### 2. Memory Safety: FFI Error Handling
*   **Risk**: Native Memory Leak
*   **Severity**: High
*   **Description**: In `lib.rs`, the FFI import logic (e.g., `from_ffi`) may take ownership of FFI structures (via `std::ptr::read`) before full validation. If a subsequent step fails and returns an error code, the ownership might be dropped without invoking the required Arrow `release` callback, leading to a leak of the underlying C-allocated memory.
*   **Required Fix**: Ensure that ownership is only taken after validation, or that explicit `release` callbacks are invoked in all error paths where ownership was assumed.

### 3. Scalability: Ack State Growth
*   **Risk**: Driver OOM / Performance Degradation
*   **Severity**: Medium (Long-running streams)
*   **Description**: The `pendingCommits` list in the Scala `PubSubMicroBatchStream` grows indefinitely with each micro-batch, as there is no feedback loop to prune "fully acknowledged" batches from the state.
*   **Required Fix**: Implement a stateful signal or custom metric to confirm when all partitions for a Batch ID have been acknowledged, allowing the Driver to prune that ID from the `pendingCommits` set.

### 4. Usability: Schema Attributes & Configuration
*   **Request**: Add support for Pub/Sub attributes in the Arrow schema.
*   **Request**: Move "Jitter" configuration (currently hardcoded 0-500ms) to `PubSubConfig`.

### 5. Code Hygiene: JNI Static Safety
*   **Risk**: Undefined Behavior / Future Compiler Error
*   **Severity**: Low (Warning)
*   **Description**: The JNI Logger in `logging.rs` uses `static mut` for the global sender, which triggers `[warn(static_mut_refs)]` in modern Rust versions. This is discouraged due to safety risks.
*   **Recommendation**: Refactor to use `std::sync::OnceLock` (stable since 1.70) or `parking_lot::RwLock` to avoid `unsafe` mutable statics.

## Verification Status
*   **JNI Panic**: The "no reactor running" panic was fixed and verified.
*   **1. Sink Reliability**: **Implemented & Verified**. `NativeWriter.close` now synchronously flushes pending messages using `WriterCommand::Flush`. Confirmed by zero-data-loss scale tests.
*   **2. Memory Safety**: **Implemented**. `lib.rs` now includes manual `release` callback invocations on all FFI error paths to prevent leaks.
*   **Throughput**: Validated at 50k messages (1KB) without regression (~40-60 MB/s).
*   **Script Reliability**: **Verified**. All shell scripts (`scripts/*.sh`) now include `trap cleanup EXIT` hooks to ensure background processes (Spark, Emulator) are killed on exit.
*   **Read Path Hardening**: **Implemented & Verified**. Synchronous subscription validation and backpressure-aware loops added to `pubsub.rs`. Verified that invalid subscriptions cause the Spark job to fail/timeout safely without silent data loss.
*   **Write Path Hardening**: **Implemented & Verified**. Fatal error discrimination added to publisher retry loop. `NativeWriter.close()` propagates flush failures. Verified that publishing to a non-existent topic causes an immediate `RuntimeException` in Spark.

## Related Artifacts
- [Rust Data Plane Details](../implementation/rust_data_plane_details.md): Details the `WriterCommand` architecture and FFI error safety patterns.
- [Stability Patterns](../implementation/stability_patterns.md): Covers "Synchronous Sink Flush" and "Manual Release on Error" patterns.
- [Spark Control Plane Details](../implementation/spark_control_plane_details.md): Documents JNI error protocols and Scala-side hardening.

## 4. Final Code & Cleanup Review (Phase 10/11)

### 4.1. Custom Metrics
- **Issue**: `PubSubCustomMetric` was missing the zero-argument constructor required by Spark UI.
- **Resolution**: Add `def this() = this("unknown", "unknown")`.

### 4.2. Repository Cleanup Checklist
- **Logs**: Delete `*.log` and `hs_err_pid*.log`.
- **Builds**: `rm -rf spark/target native/target`.
- **Metastore**: `rm -rf spark/metastore_db`.
- **Scripts**: Move ad-hoc scripts to `scripts/`.

### 4.3. Scale Hardening Learnings
- **Zombie Processes**: High-throughput tests can leave orphaned JVMs. **Solution**: Use `trap` and `pkill` in scripts.
- **Emulator Saturation**: The local emulator saturates with >10KB payloads at high frequency. **Solution**: Use real GCP Pub/Sub for high-throughput stress tests.
- **Dynamic Scaling**: The connector is stable during Swing Tests (2->10->2 executors) thanks to `jitterMs` and off-heap `ACK_RESERVOIR`.
