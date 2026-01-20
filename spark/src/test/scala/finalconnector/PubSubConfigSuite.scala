package finalconnector

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

class PubSubConfigSuite extends AnyFunSuite {

  test("getOption returns value from options map first") {
    val options = Map("testKey" -> "explicitValue")
    val result = PubSubConfig.getOption("testKey", options, null)
    assert(result.contains("explicitValue"))
  }

  test("getOption returns None if not in options and no SparkSession") {
    val options = Map.empty[String, String]
    val result = PubSubConfig.getOption("missingKey", options, null)
    assert(result.isEmpty)
  }
  
  // Note: Testing SparkSession fallback requires a full SparkSession which is heavy. 
  // skipping specific SparkConf fallback test in pure unit test to keep it fast, 
  // or we could use Mockito if available, but it's not in build.sbt.

  test("buildProcessingConfigJson generates correct JSON for simple schema") {
    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", IntegerType)
    ))
    
    val json = PubSubConfig.buildProcessingConfigJson(
      schema = schema,
      format = Some("json"),
      avroSchema = None,
      protobufDescriptor = None,
      protobufMessageName = None,
      caCertificatePath = None,
      batchSize = Some(100),
      batchBytes = None,
      batchDurationMs = None
    )
    
    // Simple string containment check to avoid JSON parsing complexity in test
    assert(json.contains(""""name":"col1""""))
    assert(json.contains(""""type":"string""""))
    assert(json.contains(""""name":"col2""""))
    assert(json.contains(""""type":"int""""))
    assert(json.contains(""""format":"json""""))
    assert(json.contains(""""batchSize":100"""))
    assert(!json.contains("avroSchema"))
  }

  test("buildProcessingConfigJson includes optional fields") {
    val schema = StructType(Seq(StructField("col1", StringType)))
    
    val json = PubSubConfig.buildProcessingConfigJson(
      schema = schema,
      format = Some("avro"),
      avroSchema = Some("{\"type\":\"record\"}"),
      protobufDescriptor = Some("desc"),
      protobufMessageName = Some("msg"),
      caCertificatePath = Some("/tmp/ca.pem"),
      batchSize = None,
      batchBytes = None,
      batchDurationMs = None
    )
    
    assert(json.contains(""""format":"avro""""))
    assert(json.contains(""""avroSchema":"{\"type\":\"record\"}""""))
    assert(json.contains(""""protobufDescriptor":"desc""""))
    assert(json.contains(""""protobufMessageName":"msg""""))
    assert(json.contains(""""caCertificatePath":"/tmp/ca.pem""""))
  }
}
