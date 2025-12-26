# Spark Pub/Sub Connector (Native Rust/Arrow)

> [!NOTE]
> **Status**: Verified on Dataproc 2.3 (Spark 3.5, Java 17). Stable for production piloting.

A high-performance, native Google Cloud Pub/Sub connector for Apache Spark (Structured Streaming), leveraging **Rust** and **Apache Arrow** for zero-copy data transfer and gRPC efficiency.

## 🚀 Key Features

- **High Throughput**: Bypasses the JVM Pub/Sub client for a native Rust implementation.
- **Zero-Copy Data Plane**: Uses the **Arrow C Data Interface** (FFI) for direct memory transfer.
- **Strictly At-Least-Once**: Native Reservoirs with background deadline management.
- **Multi-Spark Version Support**: Compatible with Spark 3.3, 3.5, and 4.0.

## 📚 Documentation

| Guide | Purpose |
| :--- | :--- |
| [**Architecture**](ARCHITECTURE.md) | Deep dive into system design, JNI engineering, and memory safety patterns. |
| [**Development**](DEVELOPMENT.md) | Instructions for building from source, running tests, and benchmarking. |
| [**Troubleshooting & Ops**](TROUBLESHOOTING.md) | **Critical** guide for JVM flags, error codes, and deployment issues. |

## ⚡ Quick Start

### 1. Prerequisites
- **Spark**: 3.5.0+ (Java 17/21)
- **Rust**: 1.75+ (for building native)

### 2. Build
```bash
# Build Rust Native Layer
cd native && cargo build --release

# Build Spark Jar
cd ../spark && java -jar sbt-launch.jar "spark35/package"
```

### 3. Usage (Structure Streaming)
```scala
// Read from Pub/Sub
val df = spark.readStream
  .format("pubsub-native")
  .option("subscriptionId", "projects/my-project/subscriptions/my-sub")
  .load()

// Write to Pub/Sub
inputDf.writeStream
  .format("pubsub-native")
  .option("topicId", "projects/my-project/topics/my-topic")
  .start()
```

### 4. Important: JVM Flags
To run this connector, you **must** allow native memory access. See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for the mandatory flags.

## ⚙️ Configuration

| Option | Description | Default |
| :--- | :--- | :--- |
| `subscriptionId` | Pub/Sub Subscription ID (Read). | **Required** |
| `topicId` | Pub/Sub Topic ID (Write). | **Required** |
| `projectId` | GCP Project ID (inferred if omitted). | *Auto* |
| `batchSize` | Messages to buffer before flush. | `1000` |
| `numPartitions` | Number of parallel read partitions. | *Cluster Cores* |

## 📄 License
Licensed under **MIT-0**. See [LICENSE](LICENSE).

## 🛠️ Common Commands Cheat Sheet

For quick copy-paste during operations.

### Environment Setup
```bash
# Generate Spark 3.5+ JVM Flags (Required for Native Access)
export FLAGS=$(./scripts/generate_spark_flags.sh)
```

### Job Management (Dataproc)
```bash
# List Active Jobs
gcloud dataproc jobs list --cluster cluster-aaf3 --region us-central1 --state-filter=active

# Kill a Job
gcloud dataproc jobs kill <JOB_ID> --cluster cluster-aaf3 --region us-central1 --quiet

# Get Driver Output URI
gcloud dataproc jobs describe <JOB_ID> --cluster cluster-aaf3 --region us-central1 --format="value(driverOutputResourceUri)"
```

### Build & Deploy
```bash
# clean and build native (use target_fix if permission issues occur)
cd native && CARGO_TARGET_DIR=target_fix cargo build --release

# clean and build spark jar
cd spark && java -jar sbt-launch.jar clean assembly

# upload jar
gsutil cp target/scala-2.12/spark-pubsub-connector-assembly-0.1.0.jar gs://pakunuru-spark-pubsub-benchmark/jars/spark-pubsub-connector-latest.jar
```

### Submit Jobs (Templates)

**Load Generator:**
```bash
gcloud dataproc jobs submit spark \
    --cluster cluster-aaf3 --region us-central1 \
    --class com.google.cloud.spark.pubsub.benchmark.PubSubLoadGenerator \
    --jars gs://pakunuru-spark-pubsub-benchmark/jars/spark-pubsub-connector-latest.jar \
    --properties "spark.driver.extraJavaOptions=$FLAGS,spark.executor.extraJavaOptions=$FLAGS,spark.driver.memory=4g,spark.jars.packages=org.apache.arrow:arrow-c-data:15.0.2,spark.pubsub.batchSize=1000" \
    -- benchmark-throughput-1kb 1000000 1024
```

**Read Benchmark:**
```bash
gcloud dataproc jobs submit spark \
    --cluster cluster-aaf3 --region us-central1 \
    --class com.google.cloud.spark.pubsub.benchmark.PubSubToGCSBenchmark \
    --jars gs://pakunuru-spark-pubsub-benchmark/jars/spark-pubsub-connector-latest.jar \
    --properties "spark.driver.extraJavaOptions=$FLAGS,spark.executor.extraJavaOptions=$FLAGS,spark.driver.memory=4g,spark.jars.packages=org.apache.arrow:arrow-c-data:15.0.2,spark.pubsub.projectId=pakunuru-1119-20250930202256" \
    -- benchmark-sub-1kb gs://pakunuru-spark-pubsub-benchmark/output/throughput_1kb
```
