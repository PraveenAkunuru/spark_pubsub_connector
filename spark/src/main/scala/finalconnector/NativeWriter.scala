package finalconnector

import org.apache.spark.internal.Logging

/**
 * JNI Bridge for high-performance Pub/Sub writing via Rust/Arrow.
 *
 * This class handles the serialization of Spark InternalRows to Arrow 
 * batches, which are then passed to the native data plane for asynchronous 
 * publishing to Google Cloud Pub/Sub.
 */
class NativeWriter extends Logging {
  NativeLoader.load()

  /**
   * Initializes a native writer for a topic.
   *
   * @param projectId          GCP Project ID
   * @param topicId            Pub/Sub Topic ID
   * @param caCertificatePath  Optional path to custom CA certs
   * @param configJson         JSON configuration string
   * @param partitionId        Spark task partition ID
   * @return A 64-bit memory address to the native Rust object
   * @throws RuntimeException if initialization fails in the native layer
   */
  @native def init(
    projectId: String, 
    topicId: String, 
    caCertificatePath: String,
    configJson: String,
    partitionId: Int): Long
  
  /**
   * Publishes a batch of Arrow data to Pub/Sub.
   *
   * @param nativePtr       Native object address
   * @param arrowArrayAddr  Address of Arrow Array FFI structure
   * @param arrowSchemaAddr Address of Arrow Schema FFI structure
   * @return Status code (0 for success)
   */
  @native def writeBatch(
    nativePtr: Long, 
    arrowArrayAddr: Long, 
    arrowSchemaAddr: Long): Int
  
  /**
   * Triggers an immediate flush of the native publisher's internal buffer.
   * 
   * @param nativePtr Native object address
   */
  @native def flush(nativePtr: Long): Unit
  
  /**
   * Closes the writer and waits for acknowledgments.
   *
   * @param nativePtr Native object address
   * @param timeoutMs Timeout for waiting for in-flight messages
   * @return Status code (0 for success, non-zero for error)
   */
  @native def close(nativePtr: Long, timeoutMs: Long): Int

  // Metrics: Directly exported from Rust atomic counters.

  /**
   * Gets total published bytes.
   * @return Total bytes sent to Pub/Sub.
   */
  @native def getPublishedBytesNative(): Long

  /**
   * Gets total published messages.
   * @return Total messages sent to Pub/Sub.
   */
  @native def getPublishedMessagesNative(): Long

  /**
   * Gets total write errors (publish failures).
   * @return Total failed publish attempts.
   */
  @native def getWriteErrorsNative(): Long

  /**
   * Gets total retry attempts.
   * @return Count of retries by the publisher client.
   */
  @native def getRetryCountNative(): Long

  /**
   * Gets average publish latency in microseconds.
   * @return Average latency in micros.
   */
  @native def getPublishLatencyMicrosNative(): Long
}
