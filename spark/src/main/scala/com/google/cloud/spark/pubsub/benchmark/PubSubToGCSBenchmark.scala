package com.google.cloud.spark.pubsub.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.util.concurrent.atomic.AtomicLong

/**
 * Benchmarks Read Throughput from Pub/Sub to GCS Parquet.
 * Usage: PubSubToGCSBenchmark <subscriptionId> <outputDir>
 */
object PubSubToGCSBenchmark {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: PubSubToGCSBenchmark <subscriptionId> <outputDir>")
      System.exit(1)
    }

    val subscriptionId = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder()
      .appName("PubSubToGCSBenchmark")
      .getOrCreate()

    // Attach Listener for Metrics
    val reportIntervalMin = sys.env.getOrElse("BENCHMARK_REPORT_INTERVAL_MIN", "10").toInt
    val listener = new BenchmarkListener(reportIntervalMin)
    spark.streams.addListener(listener)

    println(s"Starting Benchmark: Sub=$subscriptionId, Out=$outputDir")
    println(s"Reporting Interval: $reportIntervalMin minutes")
    val startTime = System.currentTimeMillis()

    val df = spark.readStream
      .format("pubsub-native")
      .option("subscriptionId", subscriptionId)
      // Use cluster default parallelism or override
      .option("numPartitions", sys.env.getOrElse("NUM_PARTITIONS", "4"))
      .load()

    val writerBase = df.writeStream
      .format("parquet")
      .option("path", outputDir)
      .option("checkpointLocation", outputDir + "_checkpoint")
    
    // Configure Trigger based on env var (default to ProcessingTime("0 seconds") for continuous run)
    val triggerMode = sys.env.getOrElse("TRIGGER_MODE", "ProcessingTime")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "0 seconds")
    
    val writerWithTrigger = if (triggerMode.equalsIgnoreCase("AvailableNow")) {
      writerBase.trigger(Trigger.AvailableNow())
    } else {
      writerBase.trigger(Trigger.ProcessingTime(triggerInterval))
    }

    val query = writerWithTrigger.start()

    query.awaitTermination()
    
    val endTime = System.currentTimeMillis()
    val durationSec = (endTime - startTime) / 1000.0

    println("=================================================")
    println(s"Benchmark Complete.")
    println(s"Duration: $durationSec seconds")
    println(s"Total Rows: ${listener.totalRows.get()}")
    // Note: listener.totalBytes is generic Spark metrics which might be 0 for some sources/sinks
    // We will rely on rows + known payload size for calculation if bytes are missing.
    println(s"Total Bytes: ${listener.totalBytes.get()}")
    println("=================================================")
    
    spark.stop()
  }
}

class BenchmarkListener(reportIntervalMin: Int) extends StreamingQueryListener {
  val totalRows = new AtomicLong(0)
  val totalBytes = new AtomicLong(0)
  
  // Reporting state
  private val startTime = System.currentTimeMillis()
  private var lastReportTime = startTime
  private val reportIntervalMs = reportIntervalMin * 60 * 1000L

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    println(s"BenchmarkListener started. Reporting every $reportIntervalMin minutes.")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val progress = event.progress
    val numRows = progress.numInputRows
    val currentTotalRows = totalRows.addAndGet(numRows)
    
    val now = System.currentTimeMillis()
    if (now - lastReportTime >= reportIntervalMs) {
      val elapsedTotalSec = (now - startTime) / 1000.0
      val avgThroughput = if (elapsedTotalSec > 0) currentTotalRows / elapsedTotalSec else 0.0
      
      println(s"--- Benchmark Status Update ---")
      println(s"Time: ${new java.util.Date(now)}")
      println(f"Elapsed: $elapsedTotalSec%.2fs")
      println(s"Total Rows: $currentTotalRows")
      println(f"Avg Throughput: $avgThroughput%.2f rows/sec")
      println(f"Current Batch Throughput: ${progress.processedRowsPerSecond}%.2f rows/sec")
      println("-------------------------------")
      
      lastReportTime = now
    }
  }
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}
