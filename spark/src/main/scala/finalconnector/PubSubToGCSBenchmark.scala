package finalconnector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types._
import java.util.concurrent.atomic.AtomicLong

/**
 * Benchmarks Read Throughput from Pub/Sub to GCS Parquet.
 * Usage: PubSubToGCSBenchmark <subscriptionId> <outputDir> [msgSizeBytes]
 */
object PubSubToGCSBenchmark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: PubSubToGCSBenchmark <subscriptionId> <outputDir> [msgSizeBytes]")
      System.exit(1)
    }

    val subscriptionId = args(0)
    val outputDir = args(1)
    val msgSizeBytes = if (args.length > 2) args(2).toLong else 1024L

    val spark = SparkSession.builder()
      .appName("PubSubToGCSBenchmark")
      .getOrCreate()

    // Attach Listener for Metrics
    val reportIntervalMin = sys.env.getOrElse("BENCHMARK_REPORT_INTERVAL_MIN", "1").toInt
    val listener = new BenchmarkListener(reportIntervalMin, msgSizeBytes)
    spark.streams.addListener(listener)

    System.err.println(s"Starting Benchmark: Sub=$subscriptionId, Out=$outputDir, MsgSize=${msgSizeBytes}B")
    System.err.println(s"Reporting Interval: $reportIntervalMin minutes")
    val startTime = System.currentTimeMillis()

    val forcedSchema = new StructType()
      .add("message_id", StringType, nullable = true)
      .add("publish_time", TimestampType, nullable = true)
      .add("payload", BinaryType, nullable = true)
      .add("ack_id", StringType, nullable = true)
      .add("attributes", org.apache.spark.sql.types.MapType(StringType, StringType), nullable = true)

    val df = spark.readStream
      .format("pubsub-native-v2")
      .schema(forcedSchema)
      .option("subscriptionId", subscriptionId)
      .load()
      .selectExpr("message_id", "publish_time", "payload", "ack_id", "attributes", "length(payload) as payload_len")

    val writerBase = df.writeStream
      .format("parquet")
      .option("path", outputDir)
      .option("checkpointLocation", outputDir + "_checkpoint")
    
    val triggerMode = sys.env.getOrElse("TRIGGER_MODE", "ProcessingTime")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "0 seconds")
    
    val writerWithTrigger = if (triggerMode.equalsIgnoreCase("AvailableNow")) {
      writerBase.trigger(Trigger.AvailableNow())
    } else {
      writerBase.trigger(Trigger.ProcessingTime(triggerInterval))
    }

    val query = writerWithTrigger.start()

    try {
      query.awaitTermination()
    } catch {
      case e: Exception => 
        System.err.println(s"Query terminated with exception: ${e.getMessage}")
    } finally {
      val endTime = System.currentTimeMillis()
      val durationSec = (endTime - startTime) / 1000.0
      val totalRows = listener.totalRows.get()
      
      val totalMb = totalRows * msgSizeBytes / (1024.0 * 1024.0)
      val avgThroughput = if (durationSec > 0) totalRows / durationSec else 0.0
      val avgMbS = if (durationSec > 0) totalMb / durationSec else 0.0

      System.err.println("=================================================")
      System.err.println(s"Benchmark Final Result")
      System.err.println(f"Duration: $durationSec%.2f seconds")
      System.err.println(s"Total Rows: $totalRows")
      System.err.println(f"Total Data: $totalMb%.2f MB")
      System.err.println(f"Avg Throughput: $avgThroughput%.2f rows/sec")
      System.err.println(f"Avg MB/s: $avgMbS%.2f MB/s")
      System.err.println("=================================================")
      spark.stop()
    }
  }
}

class BenchmarkListener(reportIntervalMin: Int, msgSizeBytes: Long) extends StreamingQueryListener {
  val totalRows = new AtomicLong(0)
  
  private val startTime = System.currentTimeMillis()
  private var lastReportTime = startTime
  private val reportIntervalMs = reportIntervalMin * 60 * 1000L

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    System.err.println(s"BenchmarkListener started. Reporting every $reportIntervalMin minutes.")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val progress = event.progress
    val numRows = progress.numInputRows
    val currentTotalRows = totalRows.addAndGet(numRows)
    
    val now = System.currentTimeMillis()
    if (now - lastReportTime >= reportIntervalMs) {
      val elapsedTotalSec = (now - startTime) / 1000.0
      val avgThroughput = if (elapsedTotalSec > 0) currentTotalRows / elapsedTotalSec else 0.0
      
      val estimatedMbTotal = currentTotalRows * msgSizeBytes / (1024.0 * 1024.0)
      val avgMbS = estimatedMbTotal / elapsedTotalSec

      System.err.println(s"--- Benchmark Status Update ---")
      System.err.println(s"Time: ${new java.util.Date(now)}")
      System.err.println(f"Elapsed: $elapsedTotalSec%.2fs")
      System.err.println(s"Total Rows: $currentTotalRows")
      System.err.println(f"Avg Throughput: $avgThroughput%.2f rows/sec ($avgMbS%.2f MB/s)")
      System.err.println(f"Current Batch Throughput: ${progress.processedRowsPerSecond}%.2f rows/sec")
      
      // Attempt to get custom metrics if available
      val metrics = progress.observedMetrics
      if (!metrics.isEmpty) {
         System.err.println(s"Custom Metrics: $metrics")
      }
      System.err.println("-------------------------------")
      
      lastReportTime = now
    }
  }
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}
