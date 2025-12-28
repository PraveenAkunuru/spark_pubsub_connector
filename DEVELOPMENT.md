# Developer Guide: Building, Testing, and Contributing

Welcome! Since this project combines **Scala (Spark)** and **Rust (Native)**, the development workflow is slightly different from a pure JVM project.

---

## 1. Prerequisites

You will need the following tools installed:
- **Java 8 or 11** (OpenJDK recommended)
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
./scripts/run_emulator_tests.sh
```
This script runs:
- **`EmulatorIntegrationTest`**: Basic read/write and metadata verification.
- **`StructuredReadTest`**: JSON/Avro parsing verification.
- **`ThroughputIntegrationTest`**: High-volume JNI bridge verification.

### 3.2. Running Scala Tests Individually
If you want to run specific tests while the emulator is already running:
```bash
cd spark
sbt "testOnly com.google.cloud.spark.pubsub.EmulatorIntegrationTest"
```

### 3.3. Rust Unit Tests
For logic inside the native data plane:
```bash
cd native
cargo test
```

---

## 4. Debugging Toolbox

### 4.1. The JNI Logging Bridge
Native logs from Rust are automatically forwarded to Spark's logger. To see detailed native logs, set your log level to `DEBUG`:
```bash
# In your spark-submit or SparkSession config
spark.driver.extraJavaOptions="-Dlog4j.configuration=file:log4j.properties"
```
Rust logs are tagged with `NativePubSub` and include partition/batch context:
`[Partition: 0, Batch: 12] Rust: fetch_batch returning 100 rows`

### 4.2. Log Correlation
Look for correlation tags in the logs to track a single message stream across the FFI boundary:
- `[Partition: X]`: Identifies the specific Spark task.
- `[Batch: Y]`: Identifies the specific micro-batch index.

### 4.3. Native Backtraces
If you encounter a native "Panic", the connector will catch it and print a full Rust backtrace to the standard error before failing the Spark task.

---

## 5. Coding Standards

- **Rust**: We follow strict `clippy` checks. Run `cargo clippy` before committing.
- **JNI Safety**: Never use `unwrap()` or `expect()` in JNI entry points in `lib.rs`. Always return a `Result` or use the `safe_jni_call!` macro.
- **Memory**: Always use `arrow-rs` FFI abstractions for zero-copy transfers to ensure correct memory release callbacks.
