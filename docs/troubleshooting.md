# Logging & Troubleshooting

## Where to find logs?

### 1. Rust Native Logs (Stderr)
Critical native events (connection status, auth failures, retries, panics) are printed to the **Standard Error (stderr)** stream.

- **In Spark UI**: Go to the **Executors** tab -> Click `stderr` log for the specific executor.
- **In Kubernetes/Docker**: View container logs (e.g., `kubectl logs <pod> stderr`).
- **Key Pattern**: Look for lines starting with `Rust:`.

### 2. Spark Connector Logs (Stdout / Log4j)
High-level lifecycle events (Writer creation, commit, abort) use Spark's standard `log4j`.

- **Location**: `stdout` / `log4j` files in Spark UI.
- **Logger Name**: `com.google.cloud.spark.pubsub.PubSubDataWriter`

## Common Log Messages

| Log Source | Message Pattern | Meaning | Action |
|------------|-----------------|---------|--------|
| **Rust** | `Rust: Async Publish failed: ... Retrying in Xms` | Transient error (e.g. 503) caught by resilience layer. | Normal behavior during high load. No action needed unless it persists > 60s. |
| **Rust** | `Service was not ready: transport error` | Connection to Pub/Sub lost or Emulator unavailable. | Check network connectivity or Emulator status. |
| **Rust** | `Rust deadline expired` | A message took too long to process. | Increase `spark.pubsub.ackDeadline` (if applicable) or verify downstream processing speed. |
| **Spark** | `NativeWriter init failed` | JNI could not initialize the Rust client. | Check `projectId`, `topicId` and credentials. Check `stderr` for specific Rust error. |

## Common Issues

### 1. "Thundering Herd" on Startup
If many executors start readers simultaneously, they might hit Pub/Sub rate limits or trigger transient connection errors.
**Fix**: Increase `spark.pubsub.jitter.ms` (default: 500ms) to spread out connection attempts.

### 2. Native Library Not Found
`java.lang.UnsatisfiedLinkError: no native_pubsub_connector in java.library.path`
**Fix**: Ensure `libnative_pubsub_connector.so` (Linux) or `.dylib` (Mac) is in a path accessible to Spark (e.g., set via `--driver-library-path` or included in the JAR/deployment).

### 3. Class Compatibility (Spark 3.3 vs 3.5)
The connector is compiled for specific Spark versions. Ensure you are using the JAR matching your Spark version (e.g., `spark-pubsub-connector-3.5_...jar` for Spark 3.5).
