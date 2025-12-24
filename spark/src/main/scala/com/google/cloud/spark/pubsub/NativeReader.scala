package com.google.cloud.spark.pubsub

/**
 * Interface to the native Rust Pub/Sub reader.
 *
 * This class uses JNI to delegate high-performance data ingestion to a Rust-based data plane.
 * It is separated from the Spark-specific stream logic to provide a clean FFI boundary.
 *
 * The native reader manages its own Tokio runtime and maintains a background StreamingPull
 * connection to Google Cloud Pub/Sub, buffering messages for low-latency retrieval.
 */
class NativeReader {
  // Ensure library is loaded when instance is created
  NativeLoader.load()

  /**
   * Initializes the native Pub/Sub client for a specific subscription.
   * Returns a raw pointer (Long) to the Rust state.
   */
  @native def init(projectId: String, subscriptionId: String): Long

  /**
   * Fetches a batch of messages from the native buffer and exports them to Arrow memory addresses.
   * Returns 1 if messages were fetched, 0 if empty, or negative on error.
   */
  @native def getNextBatch(readerPtr: Long, arrowArrayAddr: Long, arrowSchemaAddr: Long): Int

  /**
   * Sends an asynchronous Acknowledgment request for the given list of message IDs.
   */
  @native def acknowledge(readerPtr: Long, ackIds: java.util.List[String]): Int

  /**
   * Shuts down the native client and releases associated resources (runtime, connections).
   */
  @native def close(readerPtr: Long): Unit
}

object NativeReader {
  // Force loading
  def load(): Unit = {
    NativeLoader.load()
  }
}
