package com.google.cloud.spark.pubsub.benchmark

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

    println(s"Starting Load Generator: Topic=$topicId, Count=$msgCount, Size=${msgSizeBytes}B")

    // Create payload string
    val payloadString = "x" * msgSizeBytes
    val payloadBytes = payloadString.getBytes("UTF-8")

    // Generate DataFrame
    // Repartition to ensure high parallelism for write
    val df = spark.range(msgCount)
      .repartition(20) 
      .select(
        lit(payloadBytes).alias("payload")
      )

    // Write to Pub/Sub in Append mode (Batch)
    df.write
      .format("pubsub-native")
      .option("topicId", topicId)
      .option("batchSize", "2000") // Larger batches for throughput
      .option("lingerMs", "100")   // Fast flush
      .mode("append") // PubSub only supports append
      .save()

    println("Load Generation Complete.")
    spark.stop()
  }
}
