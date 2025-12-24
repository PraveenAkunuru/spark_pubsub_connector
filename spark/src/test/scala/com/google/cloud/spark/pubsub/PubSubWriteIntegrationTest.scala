package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class PubSubWriteIntegrationTest extends AnyFunSuite {

  test("Spark should write to Pub/Sub via native connector") {
    val spark = SparkSession.builder()
      .appName("PubSubWriteTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val projectId = "spark-test-project"
    val topicId = "test-topic"

    // Create a dataset with diverse types to test expanded support and attributes
    val df = Seq(
      ("msg1".getBytes("UTF-8"), 1, 100L, true, 1.1f, 1.11, "val1"),
      ("msg2".getBytes("UTF-8"), 2, 200L, false, 2.2f, 2.22, "val2"),
      ("msg3".getBytes("UTF-8"), 3, 300L, true, 3.3f, 3.33, "val3")
    ).toDF("payload", "int_val", "long_val", "bool_val", "float_val", "double_val", "custom_attr")
      .withColumn("message_id", lit(""))
      .withColumn("publish_time", current_timestamp())

    println("Scala: Starting Spark write...")
    
    try {
      df.write
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("topicId", topicId)
        .option("batchSize", "2") // Force at least one flush
        .mode("append")
        .save()
        
      println("Scala: Spark write completed successfully.")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        fail(s"Spark write failed: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}
