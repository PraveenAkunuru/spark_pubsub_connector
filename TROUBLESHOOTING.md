# Troubleshooting & Error Codes

This guide helps diagnose common issues and provides a "Rosetta Stone" for native error codes.

## 🧭 Configuration Precedence

The connector resolves settings in the following order:
1.  **Explicit `.option()`**: Values passed directly in the Spark code.
2.  **Global `--conf spark.pubsub.<key>`**: Useful for cluster-wide settings (e.g., `projectId`, `numPartitions`).
3.  **Smart Defaults**:
    - `projectId`: Inferred from `ServiceOptions` (GCP environment) or `GOOGLE_CLOUD_PROJECT`.
    - `numPartitions`: Automatically set to match the cluster's default parallelism.

## 🪨 Error Code Rosetta Stone (Native Layer)

If a Spark task fails with a negative exit code in the logs, use this table to identify the root cause:

| Code | Label | Meaning & Recommended Action |
| :--- | :--- | :--- |
| **-1** | `INVALID_PTR` | Null pointer passed to Rust. Restart the job; if persistent, check native memory logs. |
| **-2** | `ARROW_ERR` | Failed to create an Arrow `RecordBatch`. Likely a schema mismatch between Spark and the data. |
| **-3** | `FFI_ERR` | Memory handover failure via JNI. Verify `arrow-rs` version matches the JAR's Arrow version. |
| **-5** | `CONN_LOST` | The background Rust task died. Check network connectivity or GCP IAM permissions. |
| **-20** | `LAYOUT_MISMATCH`| Memory layout validation failed. Ensure the `.so`/`.dylib` matches the JAR version exactly. |
| **-100** | `NATIVE_PANIC` | A logic error occurred in Rust. Check the `stderr` for a Rust backtrace and log a bug. |

## 🛠️ Mandatory JVM Flags (Java 17+)

Apache Spark 3.5+ typically runs on Java 17. Because this connector uses the **Arrow C Data Interface** and **JNI**, you must "open" specific Java modules to the connector.

Add these to your `spark-submit` command:

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

## 🔍 Common Issues

### 1. `UnsatisfiedLinkError`
*   **Cause**: The JVM cannot find `libnative_pubsub_connector.so` (Linux) or `.dylib` (macOS).
*   **Fix**: 
    - Ensure you ran `cargo build --release`.
    - Check that the library is either in `java.library.path` or bundled in the JAR resources under the correct platform folder (e.g., `linux-x86-64`).

### 2. `InaccessibleObjectException`
*   **Cause**: Missing `--add-opens` flags on Java 17+.
*   **Fix**: Add the flags listed in the section above.

### 3. High Memory Usage (OOM)
*   **Cause**: Usually caused by orphaned native batches if `close()` isn't called.
*   **Fix**: Ensure your Spark job isn't suppressing task cleanup. The connector now includes a 30-minute TTL safety net in the native reservoir to automatically purge abandoned data.
