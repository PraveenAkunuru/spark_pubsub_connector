# Spark Pub/Sub Connector (Native Rust/Arrow)

[![Spark 3.3|3.5|4.0](https://img.shields.io/badge/Spark-3.3%20|%203.5%20|%204.0-blue.svg)](https://spark.apache.org/)
[![Rust 1.75+](https://img.shields.io/badge/Rust-1.75+-orange.svg)](https://www.rust-lang.org/)

A high-performance **Google Cloud Pub/Sub connector** for Apache Spark Structured Streaming. This connector is designed from "first principles" to bypass JVM garbage collection bottlenecks by offloading heavy gRPC I/O, Protocol Buffer parsing, and memory management to a native **Rust** data plane.

---

## 🚀 Why This Connector?

In standard Java connectors, processing millions of messages generates massive object overhead (String, ByteString, Row objects), leading to "Stop-the-World" Garbage Collection (GC) pauses that kill streaming performance.

**Our Solution:**
- **Zero-Copy Ingestion**: Data moves from the network to Spark as columnar **Apache Arrow** batches. No Java objects are created for message payloads.
- **Native Data Plane**: High-concurrency gRPC handling in Rust via `tokio` and `tonic`.
- **Intelligent Load Balancing**: Automatically plans partitions based on cluster size (`spark.executor.instances`) and maps them to independent gRPC streams.
- **Polyglot Harmony**: Combines the best of Spark's control plane (Scala) with the efficiency of Rust.

---

## 🛠️ Quick Start (3 Minutes)

You can test the connector locally using the Pub/Sub emulator—no GCP project required!

### 1. Start the Emulator
```bash
gcloud beta emulators pubsub start --host-port=localhost:8085
```

### 2. Run a Simple Stream
```scala
val df = spark.readStream
  .format("pubsub-native") // Registered Alias
  .option("projectId", "my-project")
  .option("subscriptionId", "my-sub")
  .option("emulatorHost", "localhost:8085")
  .load()

df.writeStream
  .format("console")
  .start()
```

---

## ⚙️ Operations & Tuning

This section is for SREs and Data Engineers responsible for deploying, scaling, and maintaining the connector in production (Dataproc/GKE).

### 1. Performance Tuning

The connector's performance is driven by three main settings.

#### 1.1 Parallelism (`numPartitions` vs Spark Config)
*   **Mechanism**: The connector runs one native gRPC stream per Spark Partition.
*   **Recommendation**: Set `spark.executor.instances` * `spark.executor.cores` to determine total capacity. Then let the connector auto-scale or set `.option("numPartitions", "N")` explicitly (1-2x total cores).
*   **Limit**: High parallelism (>50) on a single subscription may trigger Pub/Sub side quota throttling.

#### 1.2 Batch Size (`batchSize`)
*   **Mechanism**: The native layer aggregates messages into Arrow batches before handing them to Spark.
*   **Guidance**:
    - **1KB Messages**: Use `batchSize=50000`. (Needs high batch count to amortize JNI overhead).
    - **10KB Messages**: Use `batchSize=5000`. (Larger payloads fill buffers faster).
    *Note: The connector now includes adaptive logic, but explicit tuning often yields better stability.*

#### 1.3 Linger Time (`readWaitMs`)
*   **Mechanism**: Max time to wait for a batch to fill.
*   **Recommendation**: `100ms` for low latency, `1000ms+` for high throughput backfills.

### 2. Monitoring Metrics

The connector exposes **Custom Task Metrics** visible in the Spark UI ("Stages" tab).

| Metric | Unit | Description |
| :--- | :--- | :--- |
| `native_ingested_bytes` | Bytes | Total bytes pulled from Pub/Sub (Wire size). |
| `native_off_heap_memory` | Bytes | Current native memory used by the Rust buffer. |
| `native_unacked_messages` | Count | Messages held in Rust waiting for Spark Commit. |
| `native_publish_latency` | µs | End-to-end latency for writing a batch. |

### 3. Troubleshooting

#### "Out of Memory" (OOM)
*   **Symptom**: Executor OOM kill.
*   **Check**: `native_off_heap_memory` in Spark UI.
*   **Fix**: Lower `spark.pubsub.batchSize` or strictly limit `spark.pubsub.flowcontrol.maxBytes` (default 100MB).

#### Large Message Stalls
*   **Symptom**: Throughput drops to 0 with 10KB+ messages.
*   **Cause**: gRPC max message size limit (4MB default) or buffer fragmentation.
*   **Fix**: Set `batchSize=500-1000`.

---

## 📉 Performance Benchmarking

Run comprehensive throughput tests on Dataproc using our standardized suite:

```bash
# Run full suite (generation + read benchmark)
./scripts/benchmark/run_throughput_suite.sh \
  --project <PROJECT_ID> \
  --cluster <CLUSTER_NAME> \
  --bucket <GCS_BUCKET> \
  --msg-size 10240
```

---

## ⚖️ License
Apache License 2.0. See [LICENSE](LICENSE) for details.

---
**Transparency Note**: This project was significantly accelerated by an Agentic AI (Google DeepMind). Code and documentation contain AI-generated content verified by human engineering.
