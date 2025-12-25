# Development & Operations Guide

This guide covers everything you need to build, test, configure, and troubleshoot the Spark Pub/Sub Connector.

---

## 1. Prerequisites

- **Java**: JDK 17 or 21.
- **Rust**: Stable toolchain (1.75+).
- **Scala**: 2.12.18 / 2.13.13 (Managed by sbt).
- **Apache Spark**: 3.3, 3.5, or 4.0.
- **Google Cloud SDK**: For authentication (ADC) and Pub/Sub Emulator.

---

## 2. Building the Connector

### Step 1: Build Native Layer (Rust)
```bash
cd native && cargo build --release
```
The resulting library will be located in `native/target/release/libnative_pubsub_connector.so`.

### Step 2: Build Spark Layer (Scala)
```bash
cd spark && java -jar sbt-launch.jar "spark35/package"
```
Substitute `spark35` with `spark33` or `spark40` as needed.

---

## 3. Configuration Reference

Options are passed via `.option("key", "value")` in Spark.

| Option | Description | Default |
| :--- | :--- | :--- |
| `projectId` | GCP Project ID. | (Required) |
| `subscriptionId` | Pub/Sub Subscription ID (for Reads). | (Required for Read) |
| `topicId` | Pub/Sub Topic ID (for Writes). | (Required for Write) |
| `format` | Data format: `raw` (binary), `json`, or `avro`. | `raw` |
| `avroSchema` | Optional Avro schema string for structured parsing. | - |
| `batchSize` | Messages to buffer before flush. | `1000` |
| `maxBatchBytes` | Max batch size in bytes before flush (Sink). | `5000000` (5MB) |
| `lingerMs` | Max wait time before flushing partial batches (Sink). | `1000` (1s) |
| `numPartitions` | Number of parallel read partitions. | `1` |
| `jitterMs` | Random delay during startup to prevent API spikes. | `500` |

---

## 4. Testing & Verification

### 4.1. Unit & Linting
- **Rust**: `cd native && cargo clippy && cargo test`
- **Scala**: `cd spark && java -jar sbt-launch.jar compile`

### 4.2. Integration Testing (Emulator)
The connector supports the Google Cloud Pub/Sub Emulator for local development.

1. **Start Emulator**:
   ```bash
   gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
   ```
2. **Run Tests**:
   ```bash
   cd spark && export PUBSUB_EMULATOR_HOST=localhost:8085
   java -jar sbt-launch.jar "spark35/test"
   ```

### 4.3. Throughput Benchmarking
Use the provided script in the `scripts/` directory:
```bash
./scripts/run_custom_throughput.sh <message_size_kb> <message_count>
```

---

## 5. Troubleshooting

### Log Locations
- **Rust Logs**: Found in Executor `stderr`. Look for `Rust:` prefix.
- **Spark Logs**: Found in standard Spark `stdout` / `log4j` logs.

### Common Issues
- **UnsatisfiedLinkError**: Ensure the native library is built and `java.library.path` is set correctly, or the library is included in the JAR resources.
- **Thundering Herd**: If many executors fail with 429/ResourceExhausted at startup, increase the `jitterMs` setting.
- **Async Flush Timeout**: If the writer hangs during shutdown, check the background publisher logs in `stderr` for persistent gRPC errors.
