# Development Guide

This guide describes how to build, test, and benchmark the Spark Pub/Sub Connector.

---

## 1. Prerequisites

- **Java**: JDK 17 or 21 (Required for Spark 3.5+).
- **Rust**: Stable toolchain (1.75+).
- **Scala**: 2.12.18 (Spark 3.5) or 2.13.13 (Spark 4.0).
- **Google Cloud SDK**: For local authentication and Emulator.

---

## 2. Build Instructions

The build is a two-step process: compiling the Native Rust layer, then packaging the Spark JAR.

### Step 1: Build Native Layer
```bash
cd native
cargo build --release
```
**Artifact**: `native/target/release/libnative_pubsub_connector.so`

### Step 2: Build Spark JAR
The SBT build will automatically include the native library if correct paths are set, but for distribution, we manually verify placement.

```bash
cd spark
# For Spark 3.5 (Scala 2.12)
java -jar sbt-launch.jar "spark35/package"
```
**Artifact**: `spark/spark35/target/scala-2.12/spark-pubsub-connector-3-5_2.12-x.x.x.jar`

---

## 3. Testing Strategies

### 3.1. Rust Unit Tests
Runs standard Rust tests for logic and serialization.
```bash
cd native
cargo test
```

### 3.2. Scala Unit Tests
Runs Spark-side verifications.
```bash
cd spark
java -jar sbt-launch.jar "spark35/test"
```

### 3.3. Integration Testing (Emulator)
You can run integration tests without hitting real Cloud Pub/Sub using the emulator.

1. **Start Emulator**:
   ```bash
   gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
   ```
2. **Run Tests with Env Var**:
   ```bash
   export PUBSUB_EMULATOR_HOST=localhost:8085
   cd spark
   java -jar sbt-launch.jar "spark35/test"
   ```

---

## 4. Benchmarking

Use the provided scripts in `scripts/` to measure throughput.

### Custom Throughput Test
Generates synthetic data to measure write performance.
```bash
./scripts/run_custom_throughput.sh <msg_size_kb> <num_messages>
# Example: 1KB messages, 1 Million count
./scripts/run_custom_throughput.sh 1 1000000
```

---

## 5. Contributing
- **Linting**: Run `cargo clippy` and `sbt scalastyle` before committing.
- **Formatting**: Use `cargo fmt`.
- **Architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md) for design principles.
