# Development Guide

## Prerequisites
- **Java**: JDK 17 or 21.
- **Rust**: Stable toolchain (1.75+).
- **Scala**: 2.12.18 (for Spark 3.5).
- **Apache Spark**: 3.5.0+.
- **Google Cloud SDK**: For authentication (ADC) and Pub/Sub Emulator.
- **Docker**: Optional, for running the Emulator in a container (some update scripts might use this).

## Building the Connector

### 1. Build Native Layer (Rust)
The native library handles the heavy lifting of Pub/Sub I/O.
```bash
cd native
cargo build --release
```
*Output*: `native/target/release/libnative_pubsub_connector.so` (on Linux).

### 2. Build Spark Layer (Scala)
The Scala layer provides the DataSourceV2 implementation.
```bash
cd spark
# For Spark 3.5
java -jar sbt-launch.jar "spark35/package"
```
*Output*: `spark/spark35/target/scala-2.12/spark-pubsub-connector-3.5_2.12-0.1.0.jar`.

## Linting

### Rust
```bash
cd native
cargo clippy --all-targets --all-features -- -D warnings
```

### Scala
```bash
cd spark
$JAVA_HOME/bin/java -Xmx4g -jar sbt-launch.jar spark35/clean spark35/compile
```

## Running Tests

The project includes a comprehensive integration test suite using the Pub/Sub Emulator.

### Setup
Start the Pub/Sub Emulator:
```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
gcloud beta emulators pubsub start --host-port=0.0.0.0:8085
```

### 1. Standard Integration Tests (Read/Ack)
```bash
cd spark
"$JAVA_HOME/bin/java" -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.AckIntegrationTest"
```

### 2. Script-Based Verification
The `scripts/` directory contains helper scripts for various test scenarios.

- **Throughput Test**: `scripts/run_throughput_test.sh`
    - Spins up emulator, publishes 50k messages, consumes them.
- **Write Verification**: `scripts/run_write_verification.sh`
- **Write Scaling**: `scripts/run_write_scaling_test.sh`
- **Dynamic Scaling**: `scripts/run_dynamic_scaling_test.sh`

### 3. Benchmarking
To measure raw throughput:
```bash
scripts/run_custom_throughput.sh <payload_bytes> <msg_count>
```
Example: `scripts/run_custom_throughput.sh 1024 100000`
