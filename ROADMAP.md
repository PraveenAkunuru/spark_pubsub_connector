# Future Roadmap

This roadmap outlines the planned enhancements for the Spark Pub/Sub Connector, focusing on multi-architecture support, performance offloading, and production stability.

---

## Phase 2: Production Hardening & Multi-Environment Stability

### 1. Multi-Arch Packaging
**Goal**: Support diverse cluster environments (x86_64, aarch64) without manual library management.
- **Implementation**: Enhance `NativeLoader` to detect `os.arch` and extract the matching `.so` from the JAR's internal resource directories.

### 2. High-Performance Offloading
**Goal**: Reduce JVM CPU usage by moving more complex logic to Rust.
- **Avro Parsing**: Move full Avro deserialization into the native layer.
- **Watermark Advanced Propagation**: Pass maximum publish times from Rust back to Spark's control plane to advance watermarks more efficiently.

### 3. Dynamic Scaling Metrics
**Goal**: Support HPA/Autoscaling based on actual Pub/Sub backlog.
- **Implementation**: Map the native `getUnackedCount` metric to a Spark `CustomTaskMetric`. This allows cluster managers to scale based on message latency/backlog rather than just CPU saturation.

### 4. Lifecycle Safety
**Goal**: Prevent native resource leaks during unexpected Spark task failures.
- **Implementation**: Integrate JVM `Cleaner` or `ShutdownHook` in `NativeReader` to guarantee `close(nativePtr)` is called if the standard Spark lifecycle methods are bypassed.

### 5. Convention over Configuration (UX)
**Goal**: Reduce the configuration surface to essential keys for production.
- **Unified Schema Intelligence**: Automatically switch between raw and structured modes based on presence of `.schema()`.
- **Intelligent Default Parallelism**: Benchmarked to 2x total executor cores by default.
- **Zero-Config Authenticion**: Hardened ADC-first path for cloud environments.

### 4. Lifecycle & Fault Tolerance (Exactly-Once)
**Goal**: Ensuring zero data loss or duplication during executor failures.
- **"Hard Stop" Recovery**: Implement chaos tests that kill executors during a Spark batch and verify state recovery from checkpoints.
- **Offset/Reservoir Sync**: Optimize the native `AckReservoir` to guarantee it never acknowledges messages until Spark's commit signal is finalized.

---

## Planned Optimizations

| Area | Issue | Proposed Fix |
| :--- | :--- | :--- |
| **Watermarking** | Latency in watermark advancement. | Pass `max_publish_time` for each batch from Rust to Spark to allow eager watermark progression. |
| **Filter Pushdown** | Excessive data egress for small filters. | Implement `SupportsPushDownFilters` in Spark and map SQL `BinaryComparison` to native filter logic. |
| **Attribute Mapping** | Iterative HashMap construction in Scala is slow. | Optimize `ArrowBatchReader` conversion logic. |
| **Flush Timeout** | Sink `close()` might hang if the network is stalled. | Add a configurable timeout to the native flush signal. |
| **FFI Safety** | Manual pointer management is error-prone. | Move to a higher-level JNI abstraction if overhead permits. |

---

## Phase 3: Enterprise Hardening (External Review Feedback)

### 1. GCP Efficiency & Resilience (Spark-BigQuery Connector Pattern)
**Goal**: Match the battle-tested reliability of Google's official connectors.
- **Fail-Fast Auth**: Refactor Scala-side authentication to use `google-cloud-java` Core libraries instead of custom logic.
- **Exponential Backoff**: Implement `GcpRetryHandler` logic for `RESOURCE_EXHAUSTED` (quota) errors, replacing simple sleep-loops.
- **Predicate Pushdown**: Investigate `SupportsPushDownFilters` to map SQL filters to Pub/Sub attributes (where possible) or client-side early logic.

### 2. Off-Heap Memory Safety (Apache Gluten Pattern)
**Goal**: Prevent silent native OOMs by integrating with Spark's memory manager.
- **Native Memory Callback**: Implement a listener interface (`MemoryHolder`) in JNI.
- **Global Allocator Metrics**: Use a global allocator (jemalloc/mimalloc) in Rust that allows polling used bytes.
- **Spark Integration**: Expose `getNativeMemoryUsage()` via JNI and report it to `TaskMemoryManager`. If native usage grows too high, Spark should know to spill or throttle.
- **Exception Bridging**: Serialize full Rust stack traces into custom Spark exceptions for easier debugging.

### 3. Build & Packaging Standardization
**Goal**: Ensure compatibility across varied Dataproc/Linux environments.
- **GLIBC Versioning**: Rename native libraries to include GLIBC version (e.g., `libnative-pubsub-glibc2.31.so`) to clarify compatibility targets.
- **Smoke Tests**: Add a "Smoke Test" phase that verifies IAM permissions and native loading *before* launching heavy Spark tasks.
- **Automated Emulator CI/CD**: Standardize use of the Google Cloud Pub/Sub Emulator in the build pipeline.
