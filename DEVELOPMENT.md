# Development Guide

This guide describes how to build, test, and benchmark the Spark Pub/Sub Connector.

---

## 1. Prerequisites

- **Java**: JDK 11, 17, or 21 (Verified on Dataproc 2.3 with Java 11).
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
# Troubleshooting & Operations Guide

This document is the definitive guide for diagnosing issues, configuring the environment, and operating the Spark Pub/Sub Connector in production.

---

## 🧭 Configuration Precedence

The connector resolves settings in the following order:
1.  **Explicit `.option()`**: Values passed directly in the code (highest priority).
    - `subscriptionId`: **Required** for reads.
    - `topicId`: **Required** for writes.
2.  **Global `--conf spark.pubsub.<key>`**: Cluster-wide defaults.
    - `spark.pubsub.projectId`: Useful for multi-job clusters.
    - `spark.pubsub.numPartitions`: Defaults to cluster parallelism if unset.
3.  **Smart Defaults**:
    - `projectId`: Automatically inferred from GCP environment (metadata server) or `GOOGLE_CLOUD_PROJECT` env var.

---

## 🛠️ Mandatory JVM Flags (Java 17+)

Apache Spark 3.5+ on Dataproc 2.2+ runs on Java 17. Because this connector uses the **Arrow C Data Interface** and **JNI**, you **must** "open" specific internal Java modules to allow native access.

**Add these flags to your `spark-submit` command or `spark-defaults.conf`:**

**Simplest method (recommended):**
Use the provided helper script to generate the flags:

```bash
export SPARK_SUBMIT_OPTS="$(./scripts/generate_spark_flags.sh)"
spark-submit ...
```

**Or manually:**

```bash
--conf "spark.driver.extraJavaOptions=\
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED" \
--conf "spark.executor.extraJavaOptions=\
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
```

**Symptoms of missing flags:**
- `java.lang.reflect.InaccessibleObjectException`
- `java.lang.IllegalAccessError` accessing `DirectBuffer`

---

## ☁️ Production Operations (Dataproc / Kubernetes)

### 1. TLS & Certificate Authorities
The native Rust layer uses `tonic` (gRPC) which requires access to system root certificates.
- **Issue**: On some minimal images (like Dataproc's Debian 12), the default certificate discovery might fail with `UnknownIssuer`.
- **Solution**: The connector explicitly checks for `/etc/ssl/certs/ca-certificates.crt`. Ensure your container/image has the `ca-certificates` package installed.

### 2. Authentication & IAM
- **Scopes**: The connector requests `https://www.googleapis.com/auth/cloud-platform` and `https://www.googleapis.com/auth/pubsub`.
- **Service Account**: Verify the VM Service Account has:
    - `roles/pubsub.viewer` (to check subscription existence)
    - `roles/pubsub.subscriber` (to read messages)
    - `roles/pubsub.publisher` (to write messages)
- **Token Issue (Double-Bearer)**: If you see `ACCESS_TOKEN_TYPE_UNSUPPORTED`, it means the `Authorization` header was malformed. The connector now automatically handles tokens whether or not they include the "Bearer " prefix.

### 3. Native Library Loading
- **Linux**: Expects `libnative_pubsub_connector.so`.
- **macOS**: Expects `libnative_pubsub_connector.dylib`.
- The library is bundled in the JAR under `/resources/linux-x86-64/` (or `darwin-aarch64`).
- **Debug**: If you see `UnsatisfiedLinkError`, verify the JAR was built with the native library included (`sbt assembly` or `sbt package` after `cargo build`).

---

## 🪨 Native Error Codes

If the connector fails at the JNI layer, it returns a negative integer code.

| Code | Label | Meaning & Action |
| :--- | :--- | :--- |
| **-1** | `INVALID_PTR` | Null pointer passed to Rust. Restart the Spark Task. |
| **-2** | `ARROW_ERR` | Failed to create Arrow Batch. Check schema compatibility. |
| **-3** | `FFI_ERR` | C Data Interface failure. Verify `arrow-rs` vs Spark Arrow version match. |
| **-5** | `CONN_LOST` | Background gRPC task died. Check network/IAM. |
| **-20** | `LAYOUT_MISMATCH`| ABI mismatch. Recompile native lib against correct Spark version. |
| **-100** | `NATIVE_PANIC` | Panic caught in Rust. Check `stderr` / driver logs for backtrace. |

---

## 🔍 Common Deployment Scenarios

### High Throughput / Backpressure
- **Symptom**: "Thundering Herd" (429 Resource Exhausted) on startup.
- **Fix**: Increase `.option("jitterMs", "2000")` to stagger connection attempts across executors.

### Memory Leaks
- **Symptom**: Executor OOM.
- **Fix**: The connector uses off-heap memory. Ensure `spark.executor.memoryOverhead` is sufficient (recommend 512MB+ per core). The **Deadline Manager** has a 30-minute safe-guard to purge abandoned ack states.

### Data Not Flowing (Read)
- **Check**:
    1. Is the Subscription attached to the correct Topic?
    2. Are messages actually available? (Use `gcloud pubsub subscriptions pull --auto-ack ...`)
    3. Is `init()` returning 0? (Check driver logs for `Rust: Subscription validation failed`).
