# Restart Guide: Spark Pub/Sub Connector (Phase 10 & Hardening)

Use this guide to resume work after restarting the project environment.

## 📍 Current Status: Phase 10 Verification
We have completed the core implementation of Phase 10 (Vectorized Reader and Metrics). Stability verification is 95% complete, but a critical JNI panic was discovered in the latest throughput run.

### ✅ Completed Milestones
- **Vectorized Reader**: Implemented `PubSubColumnarPartitionReader` using `ArrowColumnVector` for zero-copy Spark processing.
- **Custom Metrics**: Implemented `pubsub_backlog_count` into Spark's metric system.
- **Scalability**: Successfully ran tests with 10 partitions and 10,000 messages against the emulator.
- **Benchmarks**: Preliminary throughput achieved ~40-60 MB/s for 1KB payloads on local machine.
- **Documentation**: Updated `README.md` and `ARCHITECTURE.md` to reflect the final architecture.
- **Hardening (In Progress)**: Rust clippy fixes for type complexity and literal formatting.

## 🛑 Identified Issues
1. **JNI Panic**: A `panic in a function that cannot unwind` was hit in `NativeReader_init`. This usually happens when a panic occurs inside an `extern "C"` function or within a `catch_unwind` block that doesn't handle a specific failure mode correctly.
   - *Log Source*: `test_output_v6.log` or previous terminal output.
2. **Resource Exhaustion**: Stuck `sbt` and `java` processes were consuming overhead. A full cleanup was initiated.

## 🚀 Resumption Steps

### 1. Environment Check
Ensure the Pub/Sub emulator can start and the native library is built.
```bash
cd native && cargo build --release
```

### 2. Immediate Fixes
- **Address JNI Panic**: Investigate `native/src/lib.rs` around line 100-150 (`NativeReader.init`). Ensure all gRPC setup and Tokio runtime initialization are correctly wrapped and don't exit the `catch_unwind` abruptly.
- **Complete Clippy**: Fix the remaining digits grouping error in `native/src/arrow_convert.rs:204`.

### 3. Final Verification
Run the throughput and scale tests to ensure no regression after the panic fix:
```bash
./run_scale_test.sh
./run_throughput_test.sh
```

### 4. Code Review & Push
- Review `PubSubMicroBatchStream.scala` for any redundant imports or commented-out metric code.
- Commit all changes and push to the remote repository.

## 📝 Performance Baseline (Last Run)
- **Message Size**: 1 KB
- **Throughput**: ~40 MB/s
- **Parallelism**: 10 Partitions
- **Environment**: Local Machine (4-core)
