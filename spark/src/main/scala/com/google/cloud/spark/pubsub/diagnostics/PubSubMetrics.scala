package com.google.cloud.spark.pubsub.diagnostics

import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.connector.metric.CustomTaskMetric

/**
 * Base class for Pub/Sub custom metrics with summation aggregation.
 */
abstract class PubSubCustomMetricBase extends CustomMetric {
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    taskMetrics.sum.toString
  }
}

/**
 * Custom metric for native off-heap memory usage.
 */
class NativeMemoryMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_off_heap_memory_bytes"
  override def description(): String = "Estimated bytes of off-heap memory used by the native data plane"
}

/**
 * Custom metric for unacknowledged message count in the native reservoir.
 */
class NativeUnackedCountMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_unacked_messages"
  override def description(): String = "Number of messages awaiting commitment in the native ack reservoir"
}

/**
 * Custom metric for ingested bytes (Cumulative Ingest Throughput).
 */
class IngestedBytesMetric extends PubSubCustomMetricBase {
  override def name(): String = "ingested_bytes"
  override def description(): String = "Cumulative bytes ingested from Pub/Sub by the native data plane"
}

/**
 * Custom metric for ingested messages.
 */
class IngestedMessagesMetric extends PubSubCustomMetricBase {
  override def name(): String = "ingested_messages"
  override def description(): String = "Cumulative messages ingested from Pub/Sub by the native data plane"
}

/**
 * Custom metric for published bytes.
 */
class PublishedBytesMetric extends PubSubCustomMetricBase {
  override def name(): String = "published_bytes"
  override def description(): String = "Cumulative bytes published to Pub/Sub by the native data plane"
}

/**
 * Custom metric for published messages.
 */
class PublishedMessagesMetric extends PubSubCustomMetricBase {
  override def name(): String = "published_messages"
  override def description(): String = "Cumulative messages published to Pub/Sub by the native data plane"
}

/**
 * Custom metric for read errors.
 */
class ReadErrorsMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_read_errors"
  override def description(): String = "Cumulative gRPC read/ack errors encountered by the native data plane"
}

/**
 * Custom metric for write errors.
 */
class WriteErrorsMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_write_errors"
  override def description(): String = "Cumulative gRPC publish errors encountered by the native data plane"
}

/**
 * Custom metric for retry count.
 */
class RetryCountMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_retry_count"
  override def description(): String = "Cumulative retry attempts for failed gRPC calls"
}

/**
 * Custom metric for publish latency (micros).
 */
class PublishLatencyMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_publish_latency_micros"
  override def description(): String = "Cumulative time (microseconds) spent in native publish calls"
}

/**
 * Custom metric for ACK latency (micros).
 */
class AckLatencyMetric extends PubSubCustomMetricBase {
  override def name(): String = "native_ack_latency_micros"
  override def description(): String = "Cumulative time (microseconds) spent in native ACK/deadline calls"
}

/**
 * Task-level implementation of custom metrics.
 */
case class PubSubTaskMetric(metricName: String, metricValue: Long) extends CustomTaskMetric {
  override def name(): String = metricName
  override def value(): Long = metricValue
}
