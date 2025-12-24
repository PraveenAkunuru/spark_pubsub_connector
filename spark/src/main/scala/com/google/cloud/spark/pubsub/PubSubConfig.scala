package com.google.cloud.spark.pubsub

object PubSubConfig {
  val PROJECT_ID_KEY = "projectId"
  val SUBSCRIPTION_ID_KEY = "subscriptionId"
  val TOPIC_ID_KEY = "topicId"
  val NUM_PARTITIONS_KEY = "numPartitions"
  val NUM_PARTITIONS_DEFAULT = "1"
  val BATCH_SIZE_KEY = "batchSize"
  val DEFAULT_BATCH_SIZE = 1000
}
