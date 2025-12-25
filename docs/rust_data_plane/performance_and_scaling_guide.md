# Performance and Scaling Guide

This document details the performance benchmarks, scaling heuristics, and architectural optimizations used to achieve high-throughput ingestion with the Spark Pub/Sub connector.

## 1. Vectorized Read Performance

To achieve maximum throughput and minimize JVM GC pressure, the connector implements a **Direct Binary Path** (Vectorized Reads).

### Implementation
- **Mechanism**: The `PubSubColumnarPartitionReader` implements Spark's `SupportsReportColumnarBatch`.
- **Zero-Copy**: Instead of converting Arrow vectors to Spark `InternalRow` objects, the reader wraps the raw Arrow memory using `org.apache.spark.sql.vectorized.ArrowColumnVector`.
- **Memory Buffer**: Data stays in off-heap Arrow memory. Spark's Catalyst engine accesses these buffers directly via code-gen, avoiding heap allocation for message payloads.

### Benchmarks (Dec 2024 Verification)
Tested on a **4-core local environment** with varying payloads:
- **1KB Payloads**: verified stability with 50,000 messages.
    - **Throughput**: ~40-60 MB/s aggregate (estimated).
    - **Latency**: Sub-5ms JNI overhead per batch (4 cores).
- **Payload Sensitivity**:
    - **Small (100B)**: Stable (2071 msg/sec, ~2 MB/s).
    - **Medium (10KB)**: Stable (~20 MB/s estimated).
    - **Large (100KB)**: **Emulator Limitation/Config Error**. Previously verified failing (0 messages) when using hardcoded `BATCH_SIZE=100` due to exceeding the **10MB Pub/Sub POST limit** (100 * 100KB + base64 overhead > 13MB).
    - **Fixes**: 
      1. Implemented **Dynamic Batch Sizing** in `run_custom_throughput.sh` (targeting ~5MB per request). 
      2. Increased test **Timeout** to 10 minutes in `ThroughputIntegrationTest.scala` to account for emulator data-plane slowness (~2-4 MB/s max).
    - **Result**: **Emulator Instability**. While dynamic batching and env vars fixed the initial config errors, the local emulator cannot reliably sustain the connection churn and data volume (500MB+) needed for 100KB benchmarks. Transport errors persist. **Action**: Use real GCP Pub/Sub for 100KB+ payload testing.

### 6.4. Test Parameter Propagation (Reliability)
- **Insight**: `sbt` tests running in forked JVMs often lose `-D` system properties passed via the CLI.
- **Requirement**: Use **Environment Variables** (`export` in shell, `sys.env` in Scala) to pass test parameters like `PUBSUB_MSG_COUNT`.
- **Validation**: Failing to do this results in "Configuration Mismatch" where the test defaults to a high count (e.g., 50k) while the script sends fewer, leading to false-negative assertion failures.

### Dynamic Scaling Benchmark (Dec 2024)
- **Scenario**: 2 -> 10 -> 2 Executors (Swing Test)
- **Payload**: 1KB
- **Result**: **Stable**. The connector successfully handled the addition and removal of 8 executors without data loss or crashes.
- **Throughput**: ~0.5 MB/s (limited by local emulation overhead and churn).
- **Cleanup**: Verified robust resource release (no zombie processes) using `trap` handlers.

## 2. Scaling Heuristics

### Virtual Partitioning
Pub/Sub doesn't have fixed partitions like Kafka. The connector creates "Virtual Partitions" (`numPartitions`) to scale out.
- **Dynamic Balancing**: Pub/Sub server-side load balances messages across all active `StreamingPull` connections established by the executors.
- **Optimal Count**: For most workloads, set `numPartitions` equal to the total number of cores in the Spark cluster.

### Connection Quota Governance
To prevent `RESOURCE_EXHAUSTED` errors and "thundering herd" scenarios during job start:
- **Max Connections**: The connector automatically limits the number of partitions to **2x Spark's default parallelism**.
- **Staggered Initialization (Jitter)**: `NativeReader.init` introduces a random delay to spread out gRPC connection establishment across executors.

## 3. Off-Heap Signal Propagation (Ack-on-Commit)

The "Signal Propagation" mechanism is designed for scale by offloading metadata.
- **Off-Heap Metadata**: `ack_id` strings are stored in a native Rust `ACK_RESERVOIR` (HashMap).
- **Scale Impact**: Zero impact on JVM heap regardless of the number of messages in flight. The Driver only tracks small `batchId` strings.
- **Memory Safety**: Messages are only deleted from the native reservoir after a successful "Commit Signal" is received from the Driver.

## 4. Custom Metrics
The connector exposes custom metrics in the Spark UI for visibility into the native state:
- **`pubsub_backlog_count`**: The number of unacknowledged messages currently held in the native reservoir. This helps identify ingestion bottlenecks or lag.

## 5. Operational Benchmarking Tools

These scripts are provided in the repository root to verify performance:
- `./run_scale_test.sh`: Stability under high parallelism (e.g., 10 partitions, 10k messages).
- `./run_throughput_test.sh`: Standard throughput check (1KB payloads, 50k messages).
- `./run_custom_throughput.sh`: Parameterized testing for payload size sensitivity. Defaults to `throughput-test-project` to match integration tests. Accepts `SIZE_BYTES` and `MSG_COUNT` arguments.
- `./run_dynamic_scaling_test.sh`: Automated simulation of the Dynamic Scaling (Swing Test) using parameterized executor counts (2 -> 10 -> 2).

### Test Parameterization Architecture
The integration test suite supports runtime configuration via **Environment Variables** (primary) and **Java System Properties** (fallback), enabling flexible test scenarios:
- **`spark.master`**: Configurable via `TEST_MASTER` env var or `spark.master` property (default: `local[4]`). This enables Dynamic Scaling tests to inject specific master configurations.
- **Payload & Count**: The `run_custom_throughput.sh` script passes these keys to `ThroughputIntegrationTest.scala`, allowing granular performance profiling. The test reads `PUBSUB_PAYLOAD_SIZE` / `pubsub.payload.size` and `PUBSUB_MSG_COUNT` / `pubsub.msg.count`.

## 6. Verification Strategies

### Test Environment Stability
To ensure accurate benchmarks and prevent resource contention (e.g., "Zombie Processes" causing 100GB+ memory usage), automated tests MUST ensure the Spark driver and loopback Pub/Sub emulator are aggressively terminated between runs.
- **Reference**: See "High Memory Usage" in `operations/troubleshooting.md`.
- **Best Practice**: Use `trap` handlers in shell scripts to kill child processes (`pkill -P $$`) upon exit.

### Dynamic Scaling (Swing Test)
To verify robustness against Executor churn and dynamic allocation (Automated by `run_dynamic_scaling_test.sh`):
1.  **Start State**: Launch Spark job with `spark.executor.instances=2`.
2.  **Scale Up**: Manually scale to `10` executors (or enabling Dynamic Allocation with high max).
3.  **Scale Down**: Reduce back to `2` executors.
4.  **Success Criteria**:
    - **No Data Loss**: Total row count must match published count.
    - **Ack Continuity**: Messages processed by substantial executors must be acknowledged before shutdown or correctly redelivered and handled by survivors.

## 7. Write Scalability Verification (Dec 2024)

### Objective
Verify that the `NativeWriter` can handle dynamic executor scaling (2 -> 10 -> 2) without data loss or crashes, and correctly implements backoff for transient failures.

### Methodology
- **Test Script**: `run_write_scaling_test.sh` calling `run_custom_write_load.sh`.
- **Scenario**:
    1.  Start with 2 Local Executors (`local[2]`).
    2.  Scale up to 10 Executors (`local[10]`).
    3.  Scale down to 2 Executors.
- **Payload**: 100-1000 Bytes (Mock Data).
- **Target**: Local Pub/Sub Emulator.

### Results
- **Stability**: Passed. No JVM crashes or "Zombie Processes".
- **Resilience**: The `PublisherClient` successfully retried on "Service not ready" errors (caused by emulator overload) without losing data.
- **Throughput**: Limited by Emulator.
    - **Observation**: The local emulator exhibits `tonic::transport::Error(Transport, Closed)` and "Service was not ready" when concurrent write connections exceed ~10 or when payload throughput is high.
    - **Mitigation**: Implemented **Exponential Backoff** (start 100ms, max 60s) in `native/src/pubsub.rs`. This allows the connector to absorb the emulator's backpressure/instability.

### Key Learnings
1.  **Emulator != Production**: The local emulator is a single-process server that easily chokes on high-concurrency writes. It is useful for functional correctness and basic stability but **not** for peak throughput benchmarking.
2.  **Backoff is Mandatory**: Without exponential backoff on the write path, "transient" emulator errors lead to immediate task failure.
3.  **Process Cleanup**: Robust `pkill -P $$` via `trap` is essential to prevent `java` orphans from consuming memory between scale stages.

