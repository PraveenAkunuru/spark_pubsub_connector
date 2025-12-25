# Spark Pub/Sub Connector (Native Rust/Arrow)

> [!WARNING]
> **STABLE DEVELOPMENT**: This project has completed its core refactoring and code review (Phase 6.0). APIs are stable, but comprehensive production soak testing is still in progress.

A high-performance, native Google Cloud Pub/Sub connector for Apache Spark (Structured Streaming), leveraging **Rust** and **Apache Arrow** for zero-copy data transfer and gRPC efficiency.

## 🚀 Key Features

- **High Throughput**: Bypasses the JVM Pub/Sub client for a native Rust implementation using gRPC (`tonic`).
- **Zero-Copy Data Plane**: Uses the **Arrow C Data Interface** (FFI) for direct memory transfer between Rust and JVM.
- **Strictly At-Least-Once**: Native Reservoirs with background deadline management prevent message expiry.
- **Multi-Spark Version Support**: Native binary portability across Spark 3.3, 3.5, and 4.0.
- **Improved Reliability**: Safe JNI bridge with panic protection and thread-safe logging.

## 📚 Documentation

The documentation has been consolidated into three core guides:

- [**Architecture**](ARCHITECTURE.md): Deep dive into system design, JNI engineering, and safety patterns.
- [**Development & Operations**](DEVELOPMENT.md): Build instructions, configuration reference, and troubleshooting.
- [**Roadmap**](ROADMAP.md): Future objectives (multi-arch, metrics, performance offloading).

## ⚡ Quick Start

### 1. Prerequisites
- **Spark**: 3.5.0+
- **Rust**: 1.75+ (for building native)
- **Java**: 17 or 21

### 2. Build
```bash
# Build Rust Native Layer
cd native && cargo build --release

# Build Spark Jar (for Spark 3.5)
cd ../spark && java -jar sbt-launch.jar "spark35/package"
```

### 3. Usage Example (Read)
```scala
val df = spark.readStream
  .format("pubsub-native")
  .option("projectId", "my-gcp-project")
  .option("subscriptionId", "my-subscription")
  .load()

df.writeStream.format("console").start()
```

### 4. Usage Example (Write)
```scala
inputDf.select("payload") // Must have 'payload' (Binary)
  .writeStream
  .format("pubsub-native")
  .option("projectId", "my-gcp-project")
  .option("topicId", "my-topic")
  .start()
```

## 📄 License
This project is licensed under the **MIT-0 (MIT No Attribution)** license. See [LICENSE](LICENSE) for details.
