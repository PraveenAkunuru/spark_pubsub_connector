# Linting and Live Testing Guide

This guide describes how to run linting and live tests for the Spark Pub/Sub Connector.

## Linting

### Rust (Native)
Run `cargo clippy` in the `native` directory to check for Rust code issues.

```bash
cd native
cargo clippy --all-targets --all-features -- -D warnings
```

### Scala (Spark)
Run `sbt compile` using the bundled `sbt-launch.jar` in the `spark` directory.
Note: You must explicitly specify the project (e.g., `spark35`) if the root project lacks dependencies.

```bash
cd spark
# Set JAVA_HOME if needed (e.g. Java 17)
# JAVA_HOME=/path/to/java-17
$JAVA_HOME/bin/java -Xmx4g -jar sbt-launch.jar spark35/clean spark35/compile
```

## Live Testing

### Throughput Test
This test spins up a local Pub/Sub emulator, publishes 50,000 messages (1KB each), and runs a Spark Streaming job to consume them. It verifies the connector's stability and JNI integration under load.

**Prerequisites:**
- Docker (for Emulator)
- Java 17 (Runtime)

**Command:**
```bash
cd scripts
./run_throughput_test.sh
```

**Expected Output:**
- "Batch 500/500..." (Publishing complete)
- Spark logs showing task completion.
- No "Rust: Panic" logs.
- Exit code 0.

### Scale Test
Verifies the connector with 10 partitions to ensure connection limits are respected.

```bash
cd scripts
./run_scale_test.sh
```
