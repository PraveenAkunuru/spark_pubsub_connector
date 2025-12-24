package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

/**
 * Integration test for verifying the scalability of the Pub/Sub writer.
 *
 * This test uses a dynamic number of executors (controlled via `TEST_MASTER` env var)
 * to verify that the `PubSubDataWriter` can handle concurrent writes without locking or memory issues.
 *
 * Usage:
 *   TEST_MASTER="local[10]" ./run_custom_write_load.sh
 */
class WriteScaleIntegrationTest extends AnyFunSuite {

  test("Write Scalability - Dynamic Executor Count") {
    val master = sys.env.getOrElse("TEST_MASTER", "local[4]")
    val projectId = sys.env.getOrElse("PUBSUB_PROJECT_ID", "write-scale-project")
    val topicId = sys.env.getOrElse("PUBSUB_TOPIC_ID", "scale-topic")
    val msgCount = sys.env.getOrElse("PUBSUB_MSG_COUNT", "10000").toLong
    
    println(s"Starting Write Test with Master: $master, Count: $msgCount")

    val spark = SparkSession.builder()
      .appName("WriteScaleIntegrationTest")
      .master(master)
      .getOrCreate()

    import spark.implicits._

    try {
      // Extract thread count from local[N] to determine partition count
      // This ensures we actually utilize the available threads
      val numThreads = if (master.startsWith("local[")) {
        val inner = master.stripPrefix("local[").stripSuffix("]")
        if (inner == "*") 4 else inner.toInt
      } else {
        4
      }
      
      // We want at least 1 partition per thread, or 10 if threads are low, to verify distribution
      val numPartitions = math.max(numThreads, 10)
      println(s"Using $numPartitions partitions for $numThreads threads.")

      // Generate data
      // We use a simple binary payload
      val df = spark.range(msgCount)
        .select(
          lit("test-payload-data").cast("binary").as("payload"),
          col("id").cast("string").as("message_id"), // Optional but good for debugging
          current_timestamp().as("publish_time")
        )
        .repartition(numPartitions)

      val startTime = System.currentTimeMillis()

      df.write
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("topicId", topicId)
        .option("batchSize", "100") // Flush often to stress writers
        .mode("append")
        .save()

      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Write Scalability Test Completed. Time: ${duration}s")
      println(s"Throughput: ${msgCount / duration} messages/sec")

    } finally {
      spark.stop()
    }
  }
}
