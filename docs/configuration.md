# Configuration Options

## Spark Connector Options

These options are passed to the connector via `.option("key", "value")` in your Spark `readStream` or `writeStream` configuration.

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `projectId` | GCP Project ID. | Yes | - |
| `subscriptionId` | Pub/Sub Subscription ID (for **Read**). | Yes (Read) | - |
| `topicId` | Pub/Sub Topic ID (for **Write**). | Yes (Write) | - |
| `batchSize` | Number of rows to buffer before flushing to the native layer. | No | `1000` |
| `credentialsFile` | Path to Service Account JSON (if not using ADC). | No | ADC (Application Default Credentials) |
| `spark.pubsub.jitter.ms` | Random jitter delay (ms) during Reader initialization to prevent "Thundering Herd" on the Pub/Sub service when many executors start simultaneously. | No | `500` |
| `maxBatchBytes` | Maximum size in bytes for a batch before flushing (Write). | No | ~5MB |
| `lingerMs` | Maximum time in milliseconds to wait before flushing a partial batch (Write). | No | `1000` (1s) |

## Parallelism Configuration

- **Read**: The number of partitions in the Spark DataFrame is determined by the `numPartitions` option (if implemented) or defaults to Spark's parallelism settings.
- **Write**: Spark's `repartition(n)` can be used before writing to control the number of concurrent writers interacting with Pub/Sub.

## Environment Variables

- `GOOGLE_APPLICATION_CREDENTIALS`: Standard GCP env var for service account credentials.
