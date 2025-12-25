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
