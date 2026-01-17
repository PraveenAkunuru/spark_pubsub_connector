# Developer Guide: Building, Testing, and Contributing

Welcome! Since this project combines **Scala (Spark)** and **Rust (Native)**, the development workflow is slightly different from a pure JVM project.

---

## 1. Prerequisites

You will need the following tools installed:
- **Java 17** (Recommended for Spark 3.5+)
- **Scala 2.12 or 2.13**
- **sbt** (Scala Build Tool)
- **Rust 1.75+** (`cargo` and `rustup`)
- **Google Cloud SDK** (for the Pub/Sub emulator)

---

## 2. Building the Project

The project is built in two stages: Native first, then Spark.

### Stage 1: Native (Rust)
This creates the shared library (`.so` or `.dylib`) that Spark will load.
```bash
cd native
cargo build --release
```
The binary will be located at `native/target/release/libnative_pubsub_connector.so`.

### Stage 2: Spark (Scala)
This compiles the Spark connector and packages it into a JAR.
```bash
cd spark
sbt assembly
```
The JAR will be at `spark/target/scala-2.12/spark-pubsub-connector-assembly-0.1.0.jar`.

---

## 3. Testing

We use a "Verify-First" approach with the Pub/Sub emulator.

### 3.1. Integration Tests (Full suite)
We provide a helper script to spin up the emulator, create resources, and run all tests.
```bash
./tests/run_emulator_tests.sh
```
This script runs:
- **`EmulatorIntegrationTest`**: Basic read/write and metadata verification.
- **`StructuredReadTest`**: JSON/Avro parsing verification.
- **`ThroughputIntegrationTest`**: High-volume JNI bridge verification.

### 3.2. Benchmarking
To run the high-throughput performance suite on a Dataproc cluster:
```bash
./scripts/benchmark/run_throughput_suite.sh --help
```

---

## 4. Debugging & Logging

### 4.1. The JNI Logging Bridge
Native logs from Rust are automatically forwarded to Spark's logger.
- **Rust Filter**: Controls what Rust logs are sent.
- **Spark Filter**: Controls what Spark displays.

To see detailed native logs:
```bash
# In your spark-submit or SparkSession config
spark.driver.extraJavaOptions="-Dlog4j.configuration=file:log4j.properties"
```
Rust logs are tagged with `[Partition: X]` or `[Sink P: X]`.

### 4.2. Backtraces
If the Rust code panics, the JNI bridge catches it and prints a full backtrace to stderr before throwing a Java RuntimeException.

---

## 5. Coding Standards

- **Rust**:
  - Run `cargo clippy --all-targets -- -D warnings` before committing.
  - Run `cargo fmt`.
  - **Safety**: Never use `unwrap()` in FFI code. Use `safe_jni_call`.

- **Scala**:
  - Run `sbt compile` to check for warnings.
  - Use `scalafmt` if configured.

---
**Transparency Note**: This project was significantly accelerated by an Agentic AI (Google DeepMind). Code and documentation contain AI-generated content verified by human engineering.
