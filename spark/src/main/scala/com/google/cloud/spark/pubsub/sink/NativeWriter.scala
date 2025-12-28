package com.google.cloud.spark.pubsub.sink

import com.google.cloud.spark.pubsub.core.NativeLoader

/**
 * Interface to the native Rust Pub/Sub publisher.
 *
 * This class uses JNI to delegate high-throughput message publishing to the Rust data plane.
 * It consumes Arrow batches directly from Spark memory, converting and publishing them
 * in native code for maximum efficiency.
 */
class NativeWriter {
  // Ensure library is loaded when instance is created
  NativeLoader.load()

  /**
   * Initializes the native Publisher client for a specific topic.
   * Returns a raw pointer (Long) to the Rust state.
   */
  @native def init(projectId: String, topicId: String, caCertificatePath: String, partitionId: Int): Long

  /**
   * Publishes a batch of data from Arrow memory addresses.
   * Returns 1 on success, or a negative error code.
   */
  @native def writeBatch(writerPtr: Long, arrowArrayAddr: Long, arrowSchemaAddr: Long): Int

  /**
   * Returns the cumulative count of bytes published to Pub/Sub.
   */
  @native def getPublishedBytesNative(): Long

  /**
   * Returns the cumulative count of messages published to Pub/Sub.
   */
  @native def getPublishedMessagesNative(): Long
  
  /**
   * Returns the cumulative count of gRPC write errors.
   */
  @native def getWriteErrorsNative(): Long

  /**
   * Returns the cumulative count of retry attempts.
   */
  @native def getRetryCountNative(): Long

  /**
   * Returns the cumulative time (microseconds) spent in native publish calls.
   */
  @native def getPublishLatencyMicrosNative(): Long

  /**
   * Flushes outstanding requests and shuts down the native publisher.
   * Returns 0 on success, or negative error code.
   */
  @native def close(writerPtr: Long, timeoutMs: Long): Int
}
