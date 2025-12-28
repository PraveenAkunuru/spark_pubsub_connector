# Operations Guide: Production, Tuning, and Monitoring

This guide is for SREs and Data Engineers responsible for deploying, scaling, and maintaining the Spark Pub/Sub Connector in production environments (e.g., Google Cloud Dataproc or GKE).

---

## 1. Performance Tuning

The connector's performance is driven by three main settings. You can tune these via `.option()` or Spark configuration.

### 1.1. Parallelism (`parallelism`)
*   **What it is**: The number of parallel Spark tasks (and native gRPC streams) created for each micro-batch.
*   **Recommendation**: Set this to **3x the number of available Spark executor cores**. This ensures high CPU utilization and masks I/O latency.
*   **Example**: `.option("parallelism", "120")`

### 1.2. Batch Size (`batchSize`)
*   **What it is**: The number of messages the native layer tries to aggregate into a single Arrow batch before passing it to Spark.
*   **Recommendation**: Aim for batches that are ~8MB to 32MB in size. 
    - For **1KB** messages: Use `batchSize=10000`
    - For **10KB** messages: Use `batchSize=1000`

### 1.3. Linger Time (`readWaitMs`)
*   **What it is**: How long (in milliseconds) the native reader should wait for a batch to fill before giving up and returning a smaller batch.
*   **Recommendation**: `100ms` for low-latency pipelines; `1000ms` for high-throughput batching.

---

## 2. Monitoring with Spark Metrics

The connector exposes internal native metrics directly via Spark's **Custom Task Metrics** API. You can view these in the Spark UI ("Stages" tab) under each task.

| Metric | Unit | Description |
| :--- | :--- | :--- |
| `native_ingested_bytes` | Bytes | Total bytes pulled from Pub/Sub by the native layer. |
| `native_off_heap_memory` | Bytes | Current native memory used for buffering messages. |
| `native_unacked_messages` | Count | Messages awaiting Spark micro-batch commitment. |
| `native_publish_latency` | µs | End-to-end latency for writing a batch to Pub/Sub. |

---

## 3. Production Deployment (Dataproc)

When running on Google Cloud Dataproc, the connector simplifies deployment by leveraging existing infrastructure.

### 3.1. Authentication
The connector uses **Google Application Default Credentials (ADC)**. 
- Ensure your Dataproc Service Account has the `roles/pubsub.subscriber` and `roles/pubsub.publisher` permissions.
- No need to provide JSON key files manually.

### 3.2. Library Loading
The JAR contains native binaries for Linux (x86_64). The connector automatically extracts and loads the correct version. No manual environment setup (like `LD_LIBRARY_PATH`) is required.

---

## 4. Troubleshooting

### 4.1. "Out of Memory" (OOM)
- If you see memory issues, check `native_off_heap_memory` in the Spark UI. 
- **Cause**: Too much pre-fetching.
- **Fix**: Lower the `parallelism` or `batchSize`.

### 4.2. Auth Failures (403 Forbidden)
- **Cause**: Service Account missing permissions or ADC not initialized.
- **Fix**: Run `gcloud auth application-default login` for local dev, or check IAM roles in GCP Console.

---

## 5. Future Roadmap

We are continuously improving the connector based on architectural guidance.

- [ ] **Phase 15: Protobuf & Avro Schema-Aware Support**: Native decoding for Protobuf/Avro payloads. 
    - See [**ProtoArchitecturalGuidance.md**](ProtoArchitecturalGuidance.md) for the full technical blueprint (Dynamic Descriptors, `prost-reflect`, and Zero-Copy).
- [ ] **Phase 16: Dynamic Backpressure**: Automatically adjusting pre-fetch buffers based on Spark's actual processing speed.
- [ ] **Phase 17: Cross-Region Optimization**: Dedicated connection pooling for high-latency cross-region ingest.
