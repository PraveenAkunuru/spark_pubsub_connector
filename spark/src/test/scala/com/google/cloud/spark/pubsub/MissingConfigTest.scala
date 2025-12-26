package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class MissingConfigTest extends AnyFunSuite {

  test("Should fail when subscriptionId is missing for read") {
    val spark = SparkSession.builder()
      .appName("MissingConfigTest")
      .master("local[1]")
      .getOrCreate()

    try {
      val e = intercept[Exception] {
        spark.readStream
          .format("pubsub-native")
          .load()
      }
      assert(e.getMessage.contains("Missing required option") || e.getCause.getMessage.contains("Missing required option"))
    } finally {
      spark.stop()
    }
  }

  test("Should fail when topicId is missing for write") {
    val spark = SparkSession.builder()
      .appName("MissingConfigTest")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._
    val df = Seq("data").toDF("value")

    try {
      val e = intercept[Exception] {
        df.writeStream
          .format("pubsub-native")
          .start()
      }
      assert(e.getMessage.contains("Missing required option") || e.getCause.getMessage.contains("Missing required option"))
    } finally {
      spark.stop()
    }
  }
}
