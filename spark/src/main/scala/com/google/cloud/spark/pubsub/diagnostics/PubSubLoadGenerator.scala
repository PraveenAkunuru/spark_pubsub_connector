package com.google.cloud.spark.pubsub.diagnostics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Generates load to Pub/Sub using the Native Connector (Write Path).
 * Usage: PubSubLoadGenerator <topicId> <msgCount> <msgSizeBytes>
 */
object PubSubLoadGenerator {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: PubSubLoadGenerator <topicId> <msgCount> <msgSizeBytes>")
      System.exit(1)
    }

    val topicId = args(0)
    val msgCount = args(1).toLong
    val msgSizeBytes = args(2).toInt

    val spark = SparkSession.builder()
      .appName("PubSubLoadGenerator")
      .getOrCreate()

    val coresPerExecutor = spark.conf.get("spark.executor.cores", "1").toInt
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt
    val totalCores = coresPerExecutor * numExecutors

    System.err.println(s"Starting Load Generator: Topic=$topicId, Count=$msgCount, Size=${msgSizeBytes}B")
    System.err.println(s"Cluster Config: Executors=$numExecutors, CoresPerExec=$coresPerExecutor (Total Cores=$totalCores)")
    
    val startTime = System.currentTimeMillis()

    // Create payload string
    val payloadString = "x" * msgSizeBytes
    val payloadBytes = payloadString.getBytes("UTF-8")

    // Generate DataFrame
    // Repartition to ensure high parallelism for write (using totalCores * 2 for good utilization)
    val df = spark.range(msgCount)
      .repartition(totalCores * 2) 
      .select(
        lit(payloadBytes).alias("payload")
      )

    // Write to Pub/Sub in Append mode (Batch)
    df.write
      .format("pubsub-native")
      .option("topicId", topicId)
      .option("batchSize", "500") // Balanced for safe 10MB limit
      .option("lingerMs", "100")   // Fast flush
      .mode("append") // PubSub only supports append
      .save()
      
    val endTime = System.currentTimeMillis()
    val durationSec = (endTime - startTime) / 1000.0
    val totalMb = (msgCount * msgSizeBytes) / (1024.0 * 1024.0)
    val throughputMbS = if (durationSec > 0) totalMb / durationSec else 0.0
    val msgsPerSec = if (durationSec > 0) msgCount / durationSec else 0.0
    
    val mbSPerCore = if (totalCores > 0) throughputMbS / totalCores else 0.0
    val msgsSPerCore = if (totalCores > 0) msgsPerSec / totalCores else 0.0

    System.err.println("=================================================")
    System.err.println("Load Generation Complete.")
    System.err.println(f"Duration: $durationSec%.2f seconds")
    System.err.println(f"Total Data: $totalMb%.2f MB")
    System.err.println(f"Avg Throughput: $throughputMbS%.2f MB/s ($mbSPerCore%.2f MB/s/core)")
    System.err.println(f"Avg Msg Rate: $msgsPerSec%.2f msgs/sec ($msgsSPerCore%.2f msgs/s/core)")
    System.err.println("=================================================")
    spark.stop()
  }
}
