package com.google.cloud.spark.pubsub.core

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.DataSourceRegister
import scala.collection.JavaConverters._

import java.util
import com.google.cloud.spark.pubsub.core.PubSubConfig

/**
 * Spark DataSource V2 TableProvider for Google Cloud Pub/Sub.
 *
 * This is the main entry point for Spark to interact with the connector. It registers the
 * "pubsub-native" short name and handles schema inference and table instantiation.
 *
 * Why it is separated:
 * This module implements the Spark-specific control plane (DataSource V2). By keeping it
 * separate from the native data plane, we can support multiple Spark versions while
 * reusing the same high-performance Rust library.
 */
class PubSubTableProvider extends TableProvider with DataSourceRegister with org.apache.spark.internal.Logging {
  
  override def shortName(): String = "pubsub-native"

  /**
   * Defines the default schema for Pub/Sub messages if one is not provided.
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    logInfo("Inferring schema for Pub/Sub (raw mode)")
    // Default schema for raw mode: payload, message_id, publish_time
    new StructType()
      .add("message_id", StringType, nullable = true)
      .add("publish_time", TimestampType, nullable = true)
      .add("payload", BinaryType, nullable = true)
      .add("ack_id", StringType, nullable = true)
  }

  /**
   * Returns a `PubSubTable` instance configured with the given schema and properties.
   */
  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    // Fail-Fast: Verify Credentials
    try {
      val creds = com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
      val scopes = creds.createScoped("https://www.googleapis.com/auth/cloud-platform").getClass.getSimpleName
      logInfo(s"Fail-Fast Auth: Successfully loaded credentials (Scopes: $scopes)")
    } catch {
      case e: Throwable =>
        throw new RuntimeException("Fail-Fast Auth: Failed to load Google Application Default Credentials. check GOOGLE_APPLICATION_CREDENTIALS or gcloud auth.", e)
    }

    val spark = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(null)
    val projId = PubSubConfig.getOption(PubSubConfig.PROJECT_ID_KEY, properties.asScala.toMap, spark).getOrElse("autodetected")
    val subId = PubSubConfig.getOption(PubSubConfig.SUBSCRIPTION_ID_KEY, properties.asScala.toMap, spark).orNull
    val topicId = PubSubConfig.getOption(PubSubConfig.TOPIC_ID_KEY, properties.asScala.toMap, spark).orNull

    if ((subId == null || subId.trim.isEmpty) && (topicId == null || topicId.trim.isEmpty)) {
      throw new IllegalArgumentException("Missing required option: Must provide either 'subscriptionId' (for read) or 'topicId' (for write).")
    }
    
    val target = if (subId != null) s"subscription $subId" else s"topic $topicId"
    logInfo(s"Creating PubSubTable (Project: $projId) for $target")
    new PubSubTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}
