package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.streaming.Trigger
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import java.util.Base64
import java.net.{HttpURLConnection, URL}

class AvroIntegrationTest extends AnyFunSuite with Matchers {

  test("Should read Native Avro data") {
    val spark = SparkSession.builder()
      .appName("AvroIntegrationTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    try {
      import spark.implicits._
      
      val projectId = "spark-test-project"
      val topicId = "avro-test-topic"
      val subscriptionId = "avro-test-sub"
      // Fallback to localhost:8085 if env not set
      val emulatorHost = sys.env.getOrElse("PUBSUB_EMULATOR_HOST", "localhost:8085")

      // 1. Define Avro Schema
      val avroSchemaStr = 
        """
        |{
        |  "type": "record",
        |  "name": "User",
        |  "fields": [
        |    {"name": "name", "type": "string"},
        |    {"name": "age", "type": "int"}
        |  ]
        |}
        """.stripMargin
      val parser = new Schema.Parser()
      val avroSchema = parser.parse(avroSchemaStr)

      // 2. Publish Data
      val user1 = new GenericData.Record(avroSchema)
      user1.put("name", "Alice")
      user1.put("age", 30)
      
      val user2 = new GenericData.Record(avroSchema)
      user2.put("name", "Bob")
      user2.put("age", 25)
      
      println(s"Publishing to $emulatorHost...")
      publishAvro(projectId, topicId, emulatorHost, avroSchema, Seq(user1, user2))
      
      // 3. Read Stream
      // We explicitly request columns "name" and "age" via Spark schema or simple select?
      // Spark schema is inferred if not provided? No, provider requires schema if not inferred?
      // Does PubSubTableProvider infer schema?
      // Usually PubSub provides (payload, attributes, etc).
      // But Native Avro parsing replaces payload with flattened columns!
      // So we must provide the schema corresponding to Avro columns.
      // Or we assume the provider supports schema inference?
      // PubSubTableProvider probably doesn't infer schema yet (unless I implemented it).
      // So I likely need to .schema(...) or rely on default schema (payload, attributes) and select?
      // Wait, if I use Native Avro, `ArrowBatchBuilder` produces flattened columns "name", "age".
      // Spark must know about "name" and "age" in the schema passed to `nativeReader.init`.
      // So I MUST provide `.schema()` to `readStream`.
      
      // Schema: name string, age int.
      // Plus metadata fields (message_id, etc) which are always present?
      // The connector appends user fields to metadata fields.
      // So user schema should technically contain them or just input fields?
      // `ArrowBatchBuilder` handles metadata fields (0-3) + user fields.
      // The `schema` argument to `init` defines user fields (structured).
      // Spark passes `schema`.
      
      import org.apache.spark.sql.types._
      val userSchema = StructType(Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      ))
      
      val df = spark.readStream
        .format("com.google.cloud.spark.pubsub.PubSubTableProvider")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .option("numPartitions", "1")
        .option("format", "avro")
        .option("avroSchema", avroSchemaStr)
        .schema(userSchema) // Explicit schema
        .load()
        
      val query = df.writeStream
        .format("memory")
        .queryName("avro_data")
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()
        
      // 4. Verify
      var foundData = false
      for (attempt <- 1 to 30) {
        if (!foundData) {
            Thread.sleep(1000)
            val count = spark.sql("SELECT * FROM avro_data").count()
            println(s"Attempt $attempt: $count rows")
            if (count >= 2) {
               foundData = true
            }
        }
      }
      
      query.stop()
      
      val results = spark.sql("SELECT name, age FROM avro_data").collect()
      results.length should be >= 2
      
      val names = results.map(_.getString(0)).toSet
      names should contain ("Alice")
      names should contain ("Bob")
      
    } finally {
      spark.stop()
    }
  }

  def publishAvro(projectId: String, topicId: String, host: String, schema: Schema, records: Seq[GenericRecord]): Unit = {
      records.foreach { record =>
          val out = new ByteArrayOutputStream()
          val encoder = EncoderFactory.get().binaryEncoder(out, null)
          val writer = new SpecificDatumWriter[GenericRecord](schema)
          writer.write(record, encoder)
          encoder.flush()
          out.close()
          
          val bytes = out.toByteArray
          val base64Data = Base64.getEncoder.encodeToString(bytes)
          
          // Publish via HTTP
          // Handle localhost:8085 vs http://localhost:8085
          val baseUrl = if (host.startsWith("http")) host else s"http://$host"
          val url = new URL(s"$baseUrl/v1/projects/$projectId/topics/$topicId:publish")
          
          val conn = url.openConnection().asInstanceOf[HttpURLConnection]
          conn.setRequestMethod("POST")
          conn.setDoOutput(true)
          conn.setRequestProperty("Content-Type", "application/json")
          
          val jsonPayload = s"""{"messages": [{"data": "$base64Data"}]}"""
          
          val os = conn.getOutputStream
          os.write(jsonPayload.getBytes("UTF-8"))
          os.close()
          
          val respCode = conn.getResponseCode
          if (respCode != 200) {
              println(s"Failed to publish to $url: $respCode")
          }
      }
  }
}
