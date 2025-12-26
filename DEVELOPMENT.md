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

### 3.3. Layered Testing Strategy

To ensure robust Spark integration, we follow a layered strategy targeting the JNI boundary and Spark engine signals.

#### Layer 1: MemoryStream Logic Validation (JVM)
Use Spark's internal `MemoryStream` for logic/transformation testing without network I/O.
- **What it covers**: SQL transformations, stateful aggregations, and business logic.
- **Simulation**: Test hundreds of scenarios (late data, malformed JSON) in seconds.

#### Layer 2: Boundary Integration (JNI)
Focus on the hand-off between JVM and Native Rust code.
- **Schema Consistency**: Verify mapping for Spark types (Timestamp, Map, Binary) to Arrow types.
- **Pass-Through Test**: Verify that records published to the Emulator remain identical (count, order, types) after crossing the JNI boundary.

#### Layer 3: Lifecycle & Fault Tolerance
Ensures the Rust layer respects Spark's Checkpointing and Commit protocols.
- **"Hard Stop" Test**: Force-kill a Spark worker and verify unacked message redelivery (Exactly-Once verification).
- **Offset Management**: Ensure Spark Batch IDs correctly map to unacked messages in Rust reservoirs.

#### Layer 4: Advanced Spark Power Features
Target features that rely heavily on the source/sink implementation:

| Feature | Testing Priority | Validation Method |
| :--- | :--- | :--- |
| **Watermarking** | High | Verify `withWatermark` drops late messages based on `publish_time`. |
| **Filter Pushdown** | High | Verify `df.filter(...)` criteria are received by the Rust reader. |
| **AQE** | Medium | Verify Spark re-optimizes join plans based on connector-reported stats. |

#### Summary Table: Testing Priorities
| Layer | Tool/Strategy | Main Objective |
| :--- | :--- | :--- |
| **Unit** | `mockall` (Rust) / Mockito (Java) | JNI safety & memory leak prevention. |
| **Integration** | Pub/Sub Emulator | End-to-end data integrity & offset commits. |
| **Compatibility** | Spark Matrix (3.3 - 4.0) | Cross-version API stability. |
| **Resilience** | Chaos Injection | Recovery after Executor/Driver failure. |

### 3.4. Automated Emulator Suite (CI/CD)
The Google Cloud Pub/Sub Emulator is mandated for all integration tests to ensure deterministic results.
- **Docker**: Run via `gcr.io/google.com/cloudsdktool/cloud-sdk`.
- **Reliability**: Eliminates cloud latency and quota-related flakes.

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
