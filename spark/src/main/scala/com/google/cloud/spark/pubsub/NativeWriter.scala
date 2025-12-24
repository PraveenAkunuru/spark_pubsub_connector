package com.google.cloud.spark.pubsub

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
  @native def init(projectId: String, topicId: String): Long

  /**
   * Publishes a batch of data from Arrow memory addresses.
   * Returns 1 on success, or a negative error code.
   */
  @native def writeBatch(writerPtr: Long, arrowArrayAddr: Long, arrowSchemaAddr: Long): Int

  /**
   * Flushes outstanding requests and shuts down the native publisher.
   */
  @native def close(writerPtr: Long): Unit
}
