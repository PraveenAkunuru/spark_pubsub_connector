package finalconnector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.types._
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.{SparkListener, SparkListenerExecutorMetricsUpdate}
import org.apache.spark.executor.ExecutorMetrics
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

class MetricsRegistry {
  private val executorMetrics = new ConcurrentHashMap[String, (Long, Long)]() // RSS, Heap

  def update(execId: String, rss: Long, heap: Long): Unit = {
    executorMetrics.put(execId, (rss, heap))
  }

  def getClusterMetrics(): (Long, Long, Int) = {
    var totalRss = 0L
    var totalHeap = 0L
    val count = executorMetrics.size()
    val iter = executorMetrics.values().iterator()
    while (iter.hasNext) {
      val (rss, heap) = iter.next()
      totalRss += rss
      totalHeap += heap
    }
    (totalRss, totalHeap, count)
  }
}

class SparkResourceListener(registry: MetricsRegistry) extends SparkListener {
  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    val execId = executorMetricsUpdate.execId
    executorMetricsUpdate.executorUpdates.values.foreach { metrics =>
      val rss = try { metrics.getMetricValue("ProcessTreeJVMRSS") } catch { case _: Exception => 0L }
      val heap = try { metrics.getMetricValue("JVMHeapMemory") } catch { case _: Exception => 0L }
      registry.update(execId, rss, heap)
    }
  }
}

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
    
    // Register Resource Listener
    val metricsRegistry = new MetricsRegistry()
    spark.sparkContext.addSparkListener(new SparkResourceListener(metricsRegistry))
    
    val listener = new BenchmarkListener(reportIntervalMin, msgSizeBytes, metricsRegistry)
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

class BenchmarkListener(reportIntervalMin: Int, msgSizeBytes: Long, metricsRegistry: MetricsRegistry) extends StreamingQueryListener {
  val totalRows = new AtomicLong(0)
  
  private val startTime = System.currentTimeMillis()
  private var lastReportTime = startTime
  private val reportIntervalMs = reportIntervalMin * 60 * 1000L

  // Metrics Access
  val osBean = java.lang.management.ManagementFactory.getOperatingSystemMXBean
  // Try to cast to com.sun.management.OperatingSystemMXBean for cpu load if available
  val sunOsBean = try {
    osBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
  } catch {
    case _: Throwable => null
  }

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

      // Collect System Metrics
      val cpuLoad = if (sunOsBean != null) f"${sunOsBean.getProcessCpuLoad * 100}%.2f%%" else "N/A"
      val memUsed = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
      val memUsedMb = memUsed / (1024 * 1024)
      val memMaxMb = Runtime.getRuntime.maxMemory() / (1024 * 1024)
      
      // Driver Metrics
      val driverCpu = if (sunOsBean != null) sunOsBean.getProcessCpuLoad * 100 else -1.0
      val runtime = Runtime.getRuntime
      val driverHeap = runtime.totalMemory() - runtime.freeMemory()
      
      // Cluster Metrics
      val (clusterRss, clusterHeap, execCount) = metricsRegistry.getClusterMetrics()
      val clusterRssMB = clusterRss / 1024 / 1024
      val clusterHeapMB = clusterHeap / 1024 / 1024
      
      System.err.println(s"--- Benchmark Status Update ---")
      System.err.println(s"Time: ${new java.util.Date(now)}")
      System.err.println(f"Elapsed: $elapsedTotalSec%.2fs")
      System.err.println(s"Total Rows: $currentTotalRows")
      System.err.println(f"Avg Throughput: $avgThroughput%.2f rows/sec ($avgMbS%.2f MB/s)")
      System.err.println(f"Current Batch Throughput: ${progress.processedRowsPerSecond}%.2f rows/sec")
      System.err.println(f"Driver CPU Load: $cpuLoad")
      System.err.println(f"Driver Heap Used: ${memUsedMb}MB / ${memMaxMb}MB")
      System.err.println(s"Cluster Active Executors: $execCount")
      System.err.println(s"Cluster Total RSS: ${clusterRssMB}MB")
      System.err.println(s"Cluster Total Heap: ${clusterHeapMB}MB")
      
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
