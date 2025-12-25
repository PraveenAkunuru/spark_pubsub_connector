package com.google.cloud.spark.pubsub

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming.Trigger

class StructuredReadTest extends AnyFunSuite with Matchers {

  test("Should support structured Schema Projection (Write -> Read)") {
    val spark = SparkSession.builder()
      .appName("StructuredReadTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._

      val projectId = "spark-test-project"
      val topicId = "struct-topic"
      val subscriptionId = "struct-sub"

      // Define Schema
      val schema = StructType(Seq(
        StructField("name", StringType, false),
        StructField("score", IntegerType, false),
        StructField("active", BooleanType, false)
      ))

      // 1. Write Structured Data
      // We generate some data
      val inputData = Seq(
        ("Alice", 100, true),
        ("Bob", 50, false),
        ("Charlie", 75, true)
      ).toDF("name", "score", "active")

      val writeQuery = inputData.write
         .format("com.google.cloud.spark.pubsub.PubSubTableProvider")
         .option("projectId", projectId)
         .option("topicId", topicId)
         .mode("append")
         .save()
      
      // writeQuery.awaitTermination() // Not needed for batch write
      
      println("Finished writing structured data to Pub/Sub.")

      // 2. Read Structured Data
      val df = spark.readStream
        .format("com.google.cloud.spark.pubsub.PubSubTableProvider")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .schema(schema) // Provide Schema!
        .load()

      val query = df.writeStream
        .format("memory")
        .queryName("struct_data")
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      // Wait for data
      var foundData = false
      for (attempt <- 1 to 30) {
        if (!foundData) {
            Thread.sleep(1000)
            val rows = spark.sql("SELECT * FROM struct_data").collect()
            val count = rows.length
            println(s"Current count at attempt $attempt: $count")
            if (count > 0) {
                rows.foreach(r => println(s"Row: $r"))
                // Verify content
                if (count >= 3) {
                    foundData = true
                }
            }
        }
      }

      query.stop()
      
      val finalRows = spark.sql("SELECT * FROM struct_data").collect()
      println(s"Final count: ${finalRows.length}")
      
      finalRows.length should be >= 3
      
      // Check for Alice
      val alice = finalRows.find(r => r.getString(0) == "Alice")
      alice should be (defined)
      alice.get.getInt(1) should be (100)
      alice.get.getBoolean(2) should be (true)

    } finally {
      spark.stop()
    }
  }
}
