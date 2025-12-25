package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import java.util.Base64
import scala.sys.process._

class AttributeMappingIntegrationTest extends AnyFunSuite with BeforeAndAfterAll {

  val projectId = "test-project"
  val topicId = "attribute-topic"
  val subId = "attribute-sub"
  val emulatorHost = "localhost:8085"

  override def beforeAll(): Unit = {
    // Create Topic and Subscription via REST (assuming emulator is up)
    Thread.sleep(2000) // Wait for emulator
    val createTopicCmd = Seq("curl", "-s", "-X", "PUT", s"http://$emulatorHost/v1/projects/$projectId/topics/$topicId")
    val createSubCmd = Seq("curl", "-s", "-X", "PUT", s"http://$emulatorHost/v1/projects/$projectId/subscriptions/$subId",
      "-H", "Content-Type: application/json",
      "-d", s"""{"topic": "projects/$projectId/topics/$topicId"}""")
    
    createTopicCmd.!
    createSubCmd.!
  }

  test("Should promote attributes to top-level columns when missing in payload") {
    val spark = SparkSession.builder()
      .appName("AttributeMappingTest")
      .master("local[2]")
      .getOrCreate()

    try {
      // 1. Publish message with JSON payload and attributes
      // Payload has "name", but missing "age" and "country".
      // "age" and "country" will be provided in attributes.
      val payload = """{"name": "Alice"}"""
      val payloadB64 = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))
      
      // JSON structure for publish: messages: [{data: ..., attributes: {age: "30", country: "US"}}]
      val publishBody = s"""
        {
          "messages": [
            {
              "data": "$payloadB64",
              "attributes": {
                "age": "30",
                "country": "US"
              }
            }
          ]
        }
      """
      
      val pubCmd = Seq("curl", "-s", "-X", "POST", s"http://$emulatorHost/v1/projects/$projectId/topics/$topicId:publish",
        "-H", "Content-Type: application/json",
        "-d", publishBody)
      pubCmd.!
      
      // 2. Read with Spark
      // Schema includes name (from payload), age (int, from attr), country (string, from attr)
      val schema = StructType(Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("country", StringType, true)
      ))

      // We use a micro-batch query or just batch read? 
      // Connector seemingly supports batch or streaming. Let's try batch if supported, or streaming with trigger once.
      // The connector seems to be mainly Structured Streaming source?
      // "Spark Pub/Sub" usually implies Streaming.
      
      val df = spark.readStream
        .format("pubsub-native")
        // options for pubsub lite or google cloud pubsub?
        // Based on other tests, it seems we use specific options.
        // Let's check PubSubPartitionReaderBase... it doesn't show options.
        // But `PubSubMicroBatchStream` exists.
        .option("projectId", projectId)
        .option("subscriptionId", subId)
        .option("pubsubEmulatorHost", emulatorHost) // If specific option exists, else env var
        .schema(schema)
        .load()

      val query = df.writeStream
        .format("memory")
        .queryName("attribute_test")
        .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
        .outputMode("append")
        .start()

      query.awaitTermination(30000) // Wait up to 30s

      
      // 3. Verify
      val result = spark.sql("select * from attribute_test").collect()
      assert(result.length >= 1, "Should have received at least 1 message")
      
      val row = result.head
      assert(row.getAs[String]("name") == "Alice")
      // Verification of Attribute Promotion:
      assert(row.getAs[Int]("age") == 30) // Should be converted to Int
      assert(row.getAs[String]("country") == "US")
      
      query.stop()
    } finally {
      spark.stop()
    }
  }
}
