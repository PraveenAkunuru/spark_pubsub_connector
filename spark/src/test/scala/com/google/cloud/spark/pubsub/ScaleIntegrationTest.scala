package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ScaleIntegrationTest extends AnyFunSuite {
  val projectId = "scale-test-project"
  val subscriptionId = "scale-sub"

  test("Fast and scalable message consumption") {
    val spark = SparkSession.builder()
      .appName("ScaleIntegrationTest")
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
          .queryName("scale_test_table")
          .start()

      // Wait for all messages (10,000)
      var count = 0L
      val startTime = System.currentTimeMillis()
      val timeout = 60000 // 60 seconds
      
      while (count < 10000 && (System.currentTimeMillis() - startTime) < timeout) {
        Thread.sleep(2000)
        count = spark.sql("SELECT * FROM scale_test_table").count()
        println(s"Observed $count messages...")
      }
      
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"Scale Test Finished: Read $count messages in $duration seconds.")
      
      query.stop()
      
      assert(count >= 10000, s"Scale test failed: Expected 10,000 but got $count")
      
    } finally {
      spark.stop()
    }
  }
}
