package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{WriteBuilder, LogicalWriteInfo}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * Represents a logical Pub/Sub source or sink in Spark.
 *
 * This class links the Spark schema to the underlying Pub/Sub resource (topic or subscription)
 * and declares the table's capabilities (e.g., Micro-Batch Reading, Streaming Writing).
 */
class PubSubTable(schema: StructType, properties: util.Map[String, String]) 
  extends Table with SupportsRead with SupportsWrite with org.apache.spark.internal.Logging {
  
  override def name(): String = properties.getOrDefault("subscriptionId", "unknown-subscription")

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(TableCapability.MICRO_BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.STREAMING_WRITE)
  }

  /**
   * Entry point for reading data from Pub/Sub.
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    logDebug(s"Creating scan builder for table: ${name()}")
    new PubSubScanBuilder(schema, options)
  }

  /**
   * Entry point for writing data to Pub/Sub.
   */
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    logDebug(s"Creating write builder for table: ${name()}")
    new PubSubWriteBuilder(schema, info.options())
  }
}

/**
 * Builder for `PubSubScan`.
 */
class PubSubScanBuilder(schema: StructType, options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PubSubScan(schema, options)
}

import org.apache.spark.sql.connector.read.streaming.MicroBatchStream

/**
 * Defines a specific scan operation on Pub/Sub, specifically for Micro-Batch streaming.
 */
class PubSubScan(schema: StructType, options: CaseInsensitiveStringMap) extends Scan {
  override def readSchema(): StructType = schema
  
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new PubSubMicroBatchStream(schema, options.asCaseSensitiveMap().asScala.toMap, checkpointLocation)
  }

  override def supportedCustomMetrics(): Array[org.apache.spark.sql.connector.metric.CustomMetric] = {
    Array(
      new NativeMemoryMetric(),
      new NativeUnackedCountMetric(),
      new IngestedBytesMetric(),
      new IngestedMessagesMetric(),
      new ReadErrorsMetric(),
      new RetryCountMetric(),
      new AckLatencyMetric()
    )
  }
}
