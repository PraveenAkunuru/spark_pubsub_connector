package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.DataSourceRegister

import java.util

class PubSubTableProvider extends TableProvider with DataSourceRegister with org.apache.spark.internal.Logging {
  
  override def shortName(): String = "pubsub-native"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    logInfo("Inferring schema for Pub/Sub (raw mode)")
    // Default schema for raw mode: payload, message_id, publish_time
    new StructType()
      .add("payload", BinaryType, nullable = false)
      .add("message_id", StringType, nullable = false)
      .add("publish_time", TimestampType, nullable = false)
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val subId = properties.getOrDefault("subscriptionId", "unknown")
    logInfo(s"Creating PubSubTable for subscription: $subId")
    new PubSubTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}
