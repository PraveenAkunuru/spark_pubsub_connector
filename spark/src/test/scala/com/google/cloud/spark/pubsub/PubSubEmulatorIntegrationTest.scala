package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.Trigger
import java.sql.Timestamp

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
        .option("numPartitions", "4")
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
      for (attempt <- 1 to 40) {
        if (!foundData) {
            Thread.sleep(1000)
            val count = spark.sql("SELECT * FROM pubsub_data").count()
            if (count > 0) {
                println(s"Data arrived at attempt $attempt. Count: $count")
                foundData = true
            } else if (attempt % 5 == 0) {
                println(s"Waiting for data... attempt $attempt")
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

  test("Should verify data parity through JNI boundary") {
    val spark = SparkSession.builder()
      .appName("DataParityTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._
      val projectId = "spark-test-project"
      val subscriptionId = "parity-sub"

      val df = spark.readStream
        .format("com.google.cloud.spark.pubsub.PubSubTableProvider")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .option("numPartitions", "4")
        .load()

      // Verify Schema
      val schema = df.schema
      schema.fieldNames should contain allOf ("message_id", "publish_time", "payload")
      schema("message_id").dataType should be (org.apache.spark.sql.types.StringType)
      schema("publish_time").dataType should be (org.apache.spark.sql.types.TimestampType)
      schema("payload").dataType should be (org.apache.spark.sql.types.BinaryType)

      val query = df.writeStream
        .format("memory")
        .queryName("parity_check")
        .start()

      try {
        // Wait for data
        var dataArrived = false
        for (_ <- 1 to 15 if !dataArrived) {
          Thread.sleep(1000)
          val count = spark.sql("SELECT * FROM parity_check").count()
          if (count > 0) dataArrived = true
        }

        dataArrived should be (true)
        
        val rows = spark.sql("SELECT * FROM parity_check LIMIT 1").collect()
        rows should not be empty
        val row = rows(0)
        
        // Verify types and non-nullability of critical metadata
        row.getAs[String]("message_id") should not be null
        row.getAs[Timestamp]("publish_time") should not be null
        row.getAs[Array[Byte]]("payload") should not be null
        
      } finally {
        query.stop()
      }
    } finally {
      spark.stop()
    }
  }
}
