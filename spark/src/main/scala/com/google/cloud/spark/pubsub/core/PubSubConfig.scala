package com.google.cloud.spark.pubsub.core

/**
 * Constants and configuration keys for the Pub/Sub connector.
 */
object PubSubConfig {
  /** Google Cloud Project ID. */
  val PROJECT_ID_KEY = "projectId"
  /** Pub/Sub Subscription ID for reading. */
  val SUBSCRIPTION_ID_KEY = "subscriptionId"
  /** Pub/Sub Topic ID for writing. */
  val TOPIC_ID_KEY = "topicId"
  /** Number of partitions to use for Spark reading. */
  val NUM_PARTITIONS_KEY = "numPartitions"
  val NUM_PARTITIONS_DEFAULT = "1"
  /** Maximum number of messages per batch. */
  val BATCH_SIZE_KEY = "batchSize"
  val DEFAULT_BATCH_SIZE = 2000 // Higher throughput default with persistence
  /** Wait time for reading from native buffer in milliseconds. */
  val READ_WAIT_MS_KEY = "readWaitMs"
  val DEFAULT_READ_WAIT_MS = "2000"
  /** Time to wait for more messages before publishing a batch. */
  val LINGER_MS_KEY = "lingerMs"
  val DEFAULT_LINGER_MS = 1000
  /** Maximum size of a single batch in bytes. */
  val MAX_BATCH_BYTES_KEY = "maxBatchBytes"
  /** Maximum jitter in milliseconds for partition reader initialization. */
  val JITTER_MS_KEY = "jitterMs"
  val DEFAULT_JITTER_MS = "500"
  /** Timeout for flushing the native publisher on close. */
  val FLUSH_TIMEOUT_MS_KEY = "flushTimeoutMs"
  val DEFAULT_FLUSH_TIMEOUT_MS = 30000L
  /** Data format (json or avro). */
  val FORMAT_KEY = "format"
  /** Optional Avro schema string. */
  val AVRO_SCHEMA_KEY = "avroSchema"
  /** Optional explicit CA certificate path. */
  val CA_CERTIFICATE_PATH_KEY = "caCertificatePath"
  /** Expected throughput in MB/s for intelligent partitioning. */
  val EXPECTED_THROUGHPUT_MB_S_KEY = "expectedThroughputMbS"
  val DEFAULT_EXPECTED_THROUGHPUT = "100"

  /**
   * Helper to retrieve a configuration value with the following precedence:
   * 1. Explicitly provided option in .option()
   * 2. Global Spark configuration (spark.pubsub.<key>)
   * 3. Environment variable or system default (for projectId)
   */
  def getOption(key: String, options: Map[String, String], sparkSession: org.apache.spark.sql.SparkSession): Option[String] = {
    options.get(key)
      .orElse {
        if (sparkSession != null) {
          try {
            sparkSession.conf.getOption(s"spark.pubsub.$key")
          } catch {
            case _: Throwable => None
          }
        } else {
          None
        }
      }
      .orElse {
        if (key == PROJECT_ID_KEY) {
          Option(System.getenv("GOOGLE_CLOUD_PROJECT"))
            .orElse(Option(System.getProperty("google.cloud.project")))
            // Fallback to cloud core if available, otherwise None
            .orElse(try {
              Option(com.google.cloud.ServiceOptions.getDefaultProjectId)
            } catch {
              case _: Throwable => None
            })
        } else {
          None
        }
      }
  }

  /**
   * Constructs the JSON configuration string used to initialize the native data plane.
   * 
   * @param schema Spark schema for projection.
   * @param format Optional data format.
   * @param avroSchema Optional Avro schema.
   * @param caCertificatePath Optional path to a CA certificate bundle.
   * @return A JSON string compatible with the Rust `ProcessingConfig`.
   */
  def buildProcessingConfigJson(
      schema: org.apache.spark.sql.types.StructType, 
      format: Option[String], 
      avroSchema: Option[String],
      caCertificatePath: Option[String]): String = {
    import org.apache.spark.sql.types._
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    val rootArgs = mapper.createObjectNode()

    val columnsArr = mapper.createArrayNode()
    schema.fields.foreach { field =>
      val fieldObj = mapper.createObjectNode()
      fieldObj.put("name", field.name)
      val typeName = field.dataType match {
        case StringType => "string"
        case IntegerType => "int"
        case LongType => "long"
        case BooleanType => "boolean"
        case FloatType => "float"
        case DoubleType => "double"
        case _ => "string" // Fallback
      }
      fieldObj.put("type", typeName)
      columnsArr.add(fieldObj)
    }
    rootArgs.set("columns", columnsArr)

    format.foreach(f => rootArgs.put("format", f))
    avroSchema.foreach(s => rootArgs.put("avroSchema", s))
    caCertificatePath.foreach(p => rootArgs.put("caCertificatePath", p))

    mapper.writeValueAsString(rootArgs)
  }
}
