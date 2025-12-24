package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger

class EmulatorIntegrationTest extends AnyFunSuite with Matchers {

  test("Should read from Pub/Sub Emulator") {
    val spark = SparkSession.builder()
      .appName("PubSubEmulatorTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._

      val projectId = "spark-test-project"
      val subscriptionId = "test-sub"

      // We need to make sure the Native library is loaded.
      // NativeReader.load() is called in partition reader, but good to ensure here if needed?
      // Actually NativeReader object calls it on init.

      val df = spark.readStream
        .format("pubsub-native") // We mapped this in PubSubTableProvider?
        // Wait, we need to register the provider or use full class name if SPI not set up.
        // Let's use full class name for safety unless we added META-INF/services (we didn't yet).
        .format("com.google.cloud.spark.pubsub.PubSubTableProvider")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .option("numPartitions", "3")
        .load()

      val query = df.writeStream
        .format("memory")
        .queryName("pubsub_data")
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      // Wait for a few seconds for data to arrive
      // The shell script publishes 10 messages.
      // We loop and check count.
      
      var foundData = false
      for (attempt <- 1 to 20) {
        if (!foundData) {
            Thread.sleep(1000)
            val count = spark.sql("SELECT * FROM pubsub_data").count()
            println(s"Current count at attempt $attempt: $count")
            if (count > 0) {
                foundData = true
            }
        }
      }

      query.stop()
      
      val finalCount = spark.sql("SELECT * FROM pubsub_data").count()
      println(s"Final count: $finalCount")
      
      finalCount should be > 0L
      // We could also check content if we parse the binary/string data
      
    } finally {
      spark.stop()
    }
  }
}
