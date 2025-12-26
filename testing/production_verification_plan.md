# Production Verification Plan (Structure Streaming & Fault Tolerance)

**Status**: To be executed after Throughput Benchmarking (Phase 1).

## 1. Test Data Strategy
**Goal**: Publish a "Golden Dataset" containing a mix of valid, late, and duplicate messages.
**Format**: JSON.

### Schema
- `deviceId`: String (for grouping/windowing)
- `timestamp`: Timestamp (ISO-8601, to test event-time watermarking)
- `reading`: Double (for calculations like averages or sums)
- `status`: String (for filtering tests)

### Edge Cases
- **Duplicates**: Publish identical `message_id` or `deviceId+timestamp` pairs.
- **Late Data**: Publish messages with timestamps older than the watermark threshold.

## 2. Comprehensive Test Scenarios

| Scenario | Objective | Spark Operations |
| :--- | :--- | :--- |
| **Integrity & Scale** | Verify 100% of messages are captured. | `count()` aggregation over a long-running stream. |
| **Event-Time Windowing** | Verify correct extraction of `publish_time` for windowing. | `withWatermark`, `groupBy(window($"publish_time", "1 minute"))`, `avg("reading")`. |
| **Deduplication** | Verify "At-Least-Once" delivery handling. | `dropDuplicates(Seq("message_id"))`. |
| **Fault Tolerance** | Confirm no data loss after intentional crash. | Stop stream, wait for data, restart from Checkpoint. |
| **Schema Projection** | Test structured parsing in native layer. | Provide DDL schema string in `.option("schema", ...)`. |

## 3. Example Pipeline Code (Scala)

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("PubSubNativeStreamingTest")
  .getOrCreate()

import spark.implicits._

// 1. Define the Schema for structured data
val userSchema = "deviceId STRING, reading DOUBLE, status STRING, eventTime TIMESTAMP"

// 2. Read from Pub/Sub in Structured Mode
val pubsubStream = spark.readStream
  .format("pubsub-native")
  .option("projectId", "your-project-id")
  .option("subscriptionId", "your-subscription-id")
  .option("schema", userSchema) // Triggers native-side parsing 
  .option("parallelism", "8")
  .load()

// 3. Streaming Operations
val processedStream = pubsubStream
  // A. Deduplicate based on Pub/Sub Message ID 
  .dropDuplicates("message_id")
  
  // B. Handle Late Data with Watermarking (10-minute threshold)
  .withWatermark("publish_time", "10 minutes")
  
  // C. Calculate 5-minute tumbling windows
  .groupBy(
    window($"publish_time", "5 minutes"),
    $"deviceId"
  )
  .agg(
    avg("reading").as("avg_reading"),
    max("publish_time").as("last_seen")
  )

// 4. Write to Parquet on GCS with Checkpointing
val query = processedStream.writeStream
  .format("parquet")
  .option("path", "gs://your-bucket/analytics/output/")
  .option("checkpointLocation", "gs://your-bucket/analytics/checkpoints/")
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("1 minute"))
  .start()

query.awaitTermination()
```

## 4. Critical Operations to Verify
1.  **Checkpointing Recovery**: Restart job and ensure it resumes from last Batch ID without skipping data.
2.  **Watermark Dropping**: Ensure late data (older than watermark) is dropped/handled correctly.
3.  **Reservoir Flushing**: Monitor `NativeLogger` to verify `ackCommitted` is called.

## 5. GCS Parquet Considerations
- **Partitioning**: Use `.partitionBy("deviceId")` for high volume.
- **Auth**: Ensure `storage.objects.create` and `get` permissions.
