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
// projectId and numPartitions are inferred automatically!
val df = spark.readStream
  .format("pubsub-native")
  .option("subscriptionId", "my-subscription")
  .load()

df.writeStream.format("console").start()
```

### 4. Usage Example (Write)
```scala
inputDf.select("payload") 
  .writeStream
  .format("pubsub-native")
  .option("topicId", "my-topic")
  .start()
```

## 🌊 Visual Data Flow Summary

To achieve high performance, the connector uses a multi-layered approach:
1.  **Native Layer (Rust)**: Pulls raw bytes from Pub/Sub and packs them into **Arrow Columns** in native memory (off-heap).
2.  **JNI Bridge**: Passes a **memory pointer** (not the data) to Spark, ensuring zero-copy handover.
3.  **Spark Layer (Scala)**: Reads directly from that pointer using the Arrow Vector API, keeping the JVM heap empty and GC pressure low.

## ⚙️ Configuration Reference

The connector supports three levels of configuration:
1.  **Explicit Options**: Provided via `.option("key", "value")`.
2.  **Global Spark Conf**: Set via `--conf spark.pubsub.<key>=<value>`.
3.  **Smart Defaults**: Automatically inferred (e.g., `projectId`).

| Category | Option | Default | Description |
| :--- | :--- | :--- | :--- |
| **Connection** | `projectId` | *Inferred* | Google Cloud Project ID. Inferred from Spark env or Dataproc. |
| (Required) | `subscriptionId` | **Required** | Pub/Sub Subscription ID (for Reads). |
| | `topicId` | **Required** | Pub/Sub Topic ID (for Writes). |
| **Performance** | `batchSize` | `1000` | Messages to buffer before flush. |
| (Optional) | `numPartitions` | *Parallelism* | Number of read partitions. Defaults to cluster cores. |
| | `lingerMs` | `1000` | Max wait time before flushing (Sink). |
| | `maxBatchBytes`| `5000000` | Max batch size in bytes (Sink). |
| **Debugging** | `jitterMs` | `500` | Random startup delay. |
| | `emulatorHost` | - | Pub/Sub Emulator host/port. |

## 🛠️ Mandatory JVM Flags (Java 17+)

If running on Java 17 or 21 (required for Spark 4.0 and often used for 3.5), you **must** add the following options to your Spark job to allow Arrow and JNI to access internal memory:

```bash
--conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
--conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
```
For a full list of optimization flags, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## 📄 License
This project is licensed under the **MIT-0 (MIT No Attribution)** license. See [LICENSE](LICENSE) for details.
