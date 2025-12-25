package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.DataSourceRegister

import java.util

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
    val subId = properties.getOrDefault("subscriptionId", "unknown")
    logInfo(s"Creating PubSubTable for subscription: $subId")
    new PubSubTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}
