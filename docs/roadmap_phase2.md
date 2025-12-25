# Spark Integration Plan (Phase 2)

With the core data plane functional, Phase 2 focuses on production hardening and multi-environment stability.

## key Objectives

### 1. Multi-Arch Packaging
**Goal**: Support diverse cluster environments (x86_64, aarch64).
*   **Technical Implementation**: Enhance `NativeLoader` to detect `os.arch` and extract the matching `.so` from a structured resources directory.

### 2. Schema-Mode Offloading
**Goal**: Native payload parsing for high performance.
*   **Technical Implementation**: Extend `ArrowBatchBuilder` to accept a JSON/Avro schema and parse the payload binary column into structured Arrow columns within Rust.

### 3. Dynamic Scaling Metrics
**Goal**: Support HPA/Autoscaling based on actual backlog.
*   **Technical Implementation**: Map the native `getUnackedCount` to a Spark `CustomTaskMetric` so cluster managers can scale based on message backlog rather than CPU.

### 4. Lifecycle Safety Nets
**Goal**: Prevent leaks on task failure.
*   **Technical Implementation**: Implement a JVM Cleaner or ShutdownHook in `NativeReader` to ensure `close(nativePtr)` is called even if the Spark partition reader is not gracefully terminated.

### 5. Watermark Optimization
**Goal**: Precise event-time tracking.
*   **Technical Implementation**: Pass the maximum `publish_time` observed in a native batch back to the `PubSubMicroBatchStream` to advance Spark watermarks without extra processing cycles.

## Improvement Suggestions

### Dynamic Attribute Mapping
*   **Issue**: Iterative HashMap construction and string conversion for non-core columns limits throughput.
*   **Fix**: Optimize `ArrowBatchReader` conversion logic.

### Synchronous Flush Timeout
*   **Issue**: `close()` in writer might hang if background task is unresponsive.
*   **Fix**: Add configurable timeout to the native flush signal wait in `close()`.
