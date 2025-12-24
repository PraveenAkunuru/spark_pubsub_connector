package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ThroughputIntegrationTest extends AnyFunSuite {
  val projectId = "throughput-test-project"
  val subscriptionId = "throughput-sub"

  test("Throughput measurement - 1KB messages") {
    val spark = SparkSession.builder()
      .appName("ThroughputIntegrationTest")
      .master("local[4]")
      .config("spark.sql.vectorized.enabled", "true")
      .getOrCreate()

    try {
      val df = spark.readStream
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .option("numPartitions", "10")
        .load()

      val query = df.writeStream
          .format("memory")
          .queryName("throughput_test_table")
          .start()

      // Target count
      val target = 50000
      var count = 0L
      val startTime = System.currentTimeMillis()
      val timeout = 120000 // 120 seconds
      
      while (count < target && (System.currentTimeMillis() - startTime) < timeout) {
        Thread.sleep(1000)
        count = spark.sql("SELECT * FROM throughput_test_table").count()
        println(s"Current Count: $count")
      }
      
      val endTime = System.currentTimeMillis()
      val durationMs = endTime - startTime
      val durationSec = durationMs / 1000.0
      
      val msgPerSec = count / durationSec
      val mbPerSec = (count * 1024.0) / (1024.0 * 1024.0) / durationSec
      
      println("======================================================")
      println(s"Throughput Results (Local Machine):")
      println(s"Total Messages: $count")
      println(s"Total Time: $durationSec seconds")
      println(f"Messages/sec: $msgPerSec%.2f")
      println(f"Throughput: $mbPerSec%.2f MB/s")
      println("======================================================")
      
      query.stop()
      
      assert(count >= target, s"Throughput test failed: Expected $target but got $count")
      
    } finally {
      spark.stop()
    }
  }
}
