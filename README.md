# Spark Pub/Sub Connector (Native Rust/Arrow)

[![Spark 3.3|3.5|4.0](https://img.shields.io/badge/Spark-3.3%20|%203.5%20|%204.0-blue.svg)](https://spark.apache.org/)
[![Rust 1.75+](https://img.shields.io/badge/Rust-1.75+-orange.svg)](https://www.rust-lang.org/)

A high-performance Google Cloud Pub/Sub connector for Apache Spark Structured Streaming. This connector is designed from "first principles" to bypass JVM garbage collection bottlenecks by offloading heavy gRPC I/O and data parsing to a native Rust data plane using Apache Arrow.

---

## 🚀 Why This Connector?

In standard Java connectors, processing millions of messages generates massive object overhead, leading to "Stop-the-World" Garbage Collection (GC) pauses that kill streaming performance.

**Our Solution:**
- **Zero-Copy Ingestion**: Data moves from the network to Spark as columnar Arrow batches. No Java objects are created for message payloads.
- **Native Data Plane**: High-concurrency gRPC handling in Rust via `tokio` and `tonic`.
- **Intelligent Load Balancing**: Automatically plans partitions based on cluster size and expected throughput.
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
  .format("pubsub-native")
  .option("projectId", "my-project")
  .option("subscriptionId", "my-sub")
  .option("emulatorHost", "localhost:8085")
  .load()

df.writeStream
  .format("console")
  .start()
```

---

## 📖 Project Documentation

We've organized our documentation to help you ramp up quickly, whether you're building a pipeline or contributing to the core.

| Document | Audience | Content |
| :--- | :--- | :--- |
| [**ARCHITECTURE.md**](ARCHITECTURE.md) | Engineers | How it works: JNI, Arrow FFI, and the "Split-Brain" design. |
| [**DEVELOPMENT.md**](DEVELOPMENT.md) | Contributors | How to build, run integration tests, and debug native code. |
| [**OPERATIONS.md**](OPERATIONS.md) | Ops / SRE | How to monitor, tune performance, and run in production (Dataproc). |

---

## 📊 Performance at a Glance

| Metric | Goal | Status |
| :--- | :--- | :--- |
| **Throughput** | 100+ MB/s per node | ✅ Verified |
| **JVM GC Time** | < 1% of total CPU | ✅ Verified |
| **Serialization** | Zero (Arrow Native) | ✅ Verified |

---

## ⚖️ License
Apache License 2.0. See [LICENSE](LICENSE) for details.
