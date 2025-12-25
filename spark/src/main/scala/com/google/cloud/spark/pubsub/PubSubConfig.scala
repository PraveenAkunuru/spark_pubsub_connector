package com.google.cloud.spark.pubsub

/**
 * Constants and configuration keys for the Pub/Sub connector.
 */
object PubSubConfig {
  val PROJECT_ID_KEY = "projectId"
  val SUBSCRIPTION_ID_KEY = "subscriptionId"
  val TOPIC_ID_KEY = "topicId"
  val NUM_PARTITIONS_KEY = "numPartitions"
  val NUM_PARTITIONS_DEFAULT = "1"
  val BATCH_SIZE_KEY = "batchSize"
  val DEFAULT_BATCH_SIZE = 1000
  val LINGER_MS_KEY = "lingerMs"
  val DEFAULT_LINGER_MS = 1000
  val MAX_BATCH_BYTES_KEY = "maxBatchBytes"
  val JITTER_MS_KEY = "jitterMs"
  val DEFAULT_JITTER_MS = "500"
  val FLUSH_TIMEOUT_MS_KEY = "flushTimeoutMs"
  val DEFAULT_FLUSH_TIMEOUT_MS = 30000L
  val FORMAT_KEY = "format"
  val AVRO_SCHEMA_KEY = "avroSchema"
}
