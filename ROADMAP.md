# Future Roadmap

This roadmap outlines the planned enhancements for the Spark Pub/Sub Connector, focusing on multi-architecture support, performance offloading, and production stability.

---

## ✅ Phase 2: Production Hardening (Completed)

- [x] **Multi-Arch Packaging**: Automated detection and extraction of platform-specific native libs from JAR.
- [x] **High-Performance Offloading**: Native JSON and Avro parsing implemented in Rust.
- [x] **Intelligent Default Parallelism**: Automatically scales read partitions based on executor cores.
- [x] **Fail-Fast Auth**: Standardized credential loading using Google Java SDK.
- [x] **Native Memory Metrics**: Instrumentation of off-heap buffers for monitoring and sizing.
- [x] **Exponential Backoff**: Robust gRPC retry logic with jitter.

---

## 🚀 Phase 3: Enterprise Scale & Resilience

### 1. Dynamic Backlog-Based Scaling
**Goal**: Support HPA/Autoscaling based on actual Pub/Sub backlog.
- **Implementation**: Map the native `getUnackedCount` metric to a Spark `CustomTaskMetric`. This allows cluster managers to scale based on message latency/backlog rather than just CPU saturation.

### 2. Lifecycle Safety (JVM Cleaner)
**Goal**: Prevent native resource leaks during unexpected Spark task failures.
- **Implementation**: Integrate JVM `Cleaner` or `ShutdownHook` in `NativeReader` to guarantee `close(nativePtr)` is called if the standard Spark lifecycle methods are bypassed.

### 3. Off-Heap Memory Safety (Spark Integration)
**Goal**: Prevent silent native OOMs by integrating with Spark's memory manager.
- **Spark Integration**: Expose `getNativeMemoryUsage()` via JNI and report it to `TaskMemoryManager`. If native usage grows too high, Spark should know to spill or throttle.

### 4. Advanced SQL Pushdown
**Goal**: Reduce data egress by filtering at the source.
- **Implementation**: Implement `SupportsPushDownFilters` in Spark and map SQL `BinaryComparison` to Pub/Sub filtering logic or early native filtering.

---

## Planned Optimizations

| Area | Issue | Proposed Fix |
| :--- | :--- | :--- |
| **Watermarking** | Latency in watermark advancement. | Pass `max_publish_time` for each batch from Rust to Spark to allow eager watermark progression. |
| **Attribute Mapping** | Iterative HashMap construction in Scala is slow. | Optimize `ArrowBatchReader` conversion logic. |
| **Flush Timeout** | Sink `close()` might hang if the network is stalled. | Add a configurable timeout to the native flush signal. |
| **FFI Safety** | Manual pointer management is error-prone. | Move to a higher-level JNI abstraction if overhead permits. |
