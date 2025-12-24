# Spark Pub/Sub Connector (Native Rust/Arrow)

A high-performance, native Google Cloud Pub/Sub connector for Apache Spark (Structured Streaming), leveraging **Rust** and **Apache Arrow** for zero-copy data transfer and gRPC efficiency.

## 🚀 Key Features
- **High Throughput**: Bypasses the JVM Pub/Sub client for a native Rust implementation using gRPC (`tonic`).
- **Zero-Copy Data Plane**: Uses the **Arrow C Data Interface** (FFI) for direct memory transfer between Rust and JVM.
- **Vectorized Reader**: Supports `ColumnarBatch` reads for maximum performance ("Direct Binary Path").
- **Strictly At-Least-Once**: Native Reservoirs with background deadline management prevent message expiry during GC or slow batches.
- **Micro-Batch Parallelism**: Configurable `numPartitions` for parallel ingestion limited only by connection quotas.
- **Multi-Spark Version Support**: Native binary portability across Spark 3.3, 3.5, and 4.0.

> [!CAUTION]
> **Work in Progress**: This project is currently in active development (Phase 5.6). APIs and configurations are subject to change. Use with caution in production environments.

A high-performance, native Google Cloud Pub/Sub connector for Apache Spark (Structured Streaming), powered by **Rust** and **Apache Arrow**.

## 🚀 Features
- **High Throughput**: Bypasses the JVM Pub/Sub client for a native Rust implementation using gRPC (`tonic`).
- **Zero-Copy**: Uses the Arrow C Data Interface (FFI) to transfer data between Rust and Spark without expensive serialization.
- **Structured Streaming**: Full support for Spark's `readStream` and `writeStream` APIs.
- **Type Safety**: Centralized mapping ensuring consistency between Spark SQL types and Pub/Sub messages.

## 📋 Prerequisites
- **Java**: JDK 17 or 21 (Tested on OpenJDK 21).
- **Rust**: Stable toolchain (1.75+).
- **Scala**: 2.12.18 (for Spark 3.5).
- **Apache Spark**: 3.5.0+.
- **Google Cloud SDK**: For authentication (ADC).

## 🛠️ Building the Connector

### 1. Build Native Layer (Rust)
The native library handles the heavy lifting of Pub/Sub I/O.
```bash
cd native
cargo build --release
```
*Output*: `native/target/release/libnative_pubsub_connector.so` (on Linux).

### 2. Build Spark Layer (Scala)
The Scala layer provides the DataSourceV2 implementation. Projects are organized by Spark version (`spark33`, `spark35`, `spark40`).
```bash
cd spark
# For Spark 3.5 (default for most environments)
java -jar sbt-launch.jar "spark35/package"
```
*Output*: `spark/spark35/target/scala-2.12/spark-pubsub-connector-3.5_2.12-0.1.0.jar`.

---

## 💻 Usage

### Reading from Pub/Sub
Read messages from a subscription as a structured stream.

```scala
val df = spark.readStream
  .format("pubsub-native")
  .option("projectId", "my-gcp-project")
  .option("subscriptionId", "my-subscription")
  .load()

// Schema is fixed:
// root
//  |-- message_id: string
//  |-- publish_time: timestamp
//  |-- payload: binary
//  |-- ordering_key: string
//  |-- [attributes]: map<string, string> (Optional, if configured)

df.writeStream
  .format("console")
  .start()
```

### Writing to Pub/Sub
Write a streaming DataFrame to a Pub/Sub topic.

```scala
val inputDf = ... // Must contain a 'payload' column (Binary)

inputDf.select("payload") // Optional: add 'ordering_key' or attribute columns
  .writeStream
  .format("pubsub-native")
  .option("projectId", "my-gcp-project")
  .option("topicId", "my-topic")
  .option("batchSize", "1000") // Flush to native layer every 1000 rows
  .option("lingerMs", "1000") // Or after 1 second
  .option("maxBatchBytes", "5242880") // Or if batch reaches 5MB
  .start()
```

### 📈 Metrics
The connector exposes custom metrics in the Spark UI:
- `pubsub_backlog_count`: The number of unacknowledged messages currently in the native reservoir.
```

## ⚙️ Configuration Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `projectId` | GCP Project ID. | Yes | - |
| `subscriptionId` | Pub/Sub Subscription ID (for Read). | Yes (Read) | - |
| `topicId` | Pub/Sub Topic ID (for Write). | Yes (Write) | - |
| `batchSize` | Number of rows to buffer before native write. | No | 1000 |
| `credentialsFile` | Path to Service Account JSON (if not using ADC). | No | ADC |

## 🧪 Running Tests
The project includes a comprehensive integration test suite using the Pub/Sub Emulator.

```bash
# 1. Start Emulator
export PUBSUB_EMULATOR_HOST=localhost:8085
gcloud beta emulators pubsub start --host-port=0.0.0.0:8085

# 2. Run Integration Tests
cd spark
java -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.AckIntegrationTest"
```

## 📊 Benchmark
Tested on a standard local machine (4-core), the connector achieves:
- **Throughput**: ~40-60 MB/s for 1KB payloads.
- **Latency**: Sub-5ms JNI overhead per batch.

## ⚠️ Important Notes
- **Memory Management**: This connector uses off-heap memory via Apache Arrow. Ensure your Spark Executors have sufficient memory overhead.
- **Java 21 Support**: Fully supported with "Peek" semantics (no Double-Free crashes).

## 📄 License
This project is licensed under the **MIT-0 (MIT No Attribution)** license.
- **No Support**: This software is provided "as-is" without any warranty.
- **No Liability**: The authors are not liable for any damages arising from its use.
- **Attribution**: Not required, but appreciated.

See [LICENSE](LICENSE) for details.
