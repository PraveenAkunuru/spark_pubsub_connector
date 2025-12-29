package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import scala.sys.process._

class AckIntegrationTest extends AnyFunSuite {
  val projectId = "test-project"
  val topicId = "ack-test-topic"
  val subscriptionId = "ack-test-sub"

  test("Spark should acknowledge messages after reading") {
    val spark = SparkSession.builder()
      .appName("AckIntegrationTest")
      .master("local[2]")
      .getOrCreate()

    try {
      // 1. Publish messages
      // We assume the emulator is running and topic/sub exist (setup by script)
      // But we might need to ensure they are fresh or empty.
      // For simplicity, we just rely on the script setup OR we can just check logs.
      
      // Let's rely on the fact that if we read them, and then try to read AGAIN, we should get 0?
      // But "read again" in Spark might reuse offsets or something?
      // No, we are in "raw" mode (MicroBatchStream implementation is naive).
      // It uses `StreamingPull`.
      // If messages are Acked, StreamingPull won't re-deliver them.
      
      val df = spark.readStream
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .load()

      val query = df.writeStream
          .format("memory")
          .queryName("ack_test_table")
          .start()

      // 1. Wait for messages to be processed
      var count = 0L
      for (i <- 1 to 20) {
        if (count < 10) {
          Thread.sleep(1000)
          count = spark.sql("SELECT * FROM ack_test_table").count()
          println(s"First Read Count at attempt $i: $count")
        }
      }
      
      assert(count >= 10, s"Should have read at least 10 messages, but found $count")
      
      // Stop the query to trigger PartitionReader.close() which sends Acks
      query.stop()
      
      // 2. Wait a bit for Acks to be processed in background in the emulator
      Thread.sleep(3000)
      
      // 3. Verify messages are gone by reading AGAIN
      // We start a NEW stream. If Acks worked, it should find nothing.
      val df2 = spark.readStream
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .load()

      val query2 = df2.writeStream
          .format("memory")
          .queryName("ack_test_table_2")
          .start()

      // Wait a few seconds to confirm no new messages are delivered
      var count2 = 0L
      for (i <- 1 to 5) {
        Thread.sleep(1000)
        count2 = spark.sql("SELECT * FROM ack_test_table_2").count()
        println(s"Second Read Count at attempt $i: $count2")
      }
      
      query2.stop()
      assert(count2 == 0, s"Messages should have been acked, but found $count2 in second read")

    } finally {
      spark.stop()
    }
  }
}
