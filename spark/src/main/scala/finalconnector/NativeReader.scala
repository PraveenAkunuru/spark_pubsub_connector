package finalconnector

import org.apache.spark.internal.Logging

/**
 * JNI Bridge for high-performance Pub/Sub reading via Rust/Arrow.
 *
 * This class defines the native interface used by Spark executors to reach 
 * into the Rust data plane. Data transfer occurs over the Arrow FFI 
 * (C-Data Interface) using memory addresses to avoid serialization and 
 * JNI overhead for the actual data vectors.
 *
 * ## LifeCycle
 * The native library is loaded once per JVM via the static initializer link.
 */
class NativeReader extends Logging {
  NativeLoader.load()

  /**
   * Initializes a native partition reader.
   *
   * @param projectId      GCP Project ID
   * @param subscriptionId Pub/Sub Subscription ID
   * @param jitterMillis   Wait jitter to prevent thundering herds on restarts
   * @param schemaJson     Target Arrow schema in JSON format
   * @param partitionId    Spark partition index
   * @return A 64-bit memory address (pointer) to the native Rust object
   */
  @native def init(
    projectId: String, 
    subscriptionId: String, 
    jitterMillis: Int, 
    schemaJson: String,
    partitionId: Int): Long

  /**
   * Fetches the next batch of messages from the native buffer.
   *
   * @param nativePtr       Native object address returned by `init`
   * @param batchId         Spark-level identifier for the current batch
   * @param arrowArrayAddr  Target address for Arrow Array FFI structure
   * @param arrowSchemaAddr Target address for Arrow Schema FFI structure
   * @param maxMessages     Max messages to pack into the batch
   * @param waitMs          Max time to wait for data (ms)
   * @return Number of records fetched, or negative code on error
   */
  @native def getNextBatch(
    nativePtr: Long, 
    batchId: String, 
    arrowArrayAddr: Long, 
    arrowSchemaAddr: Long, 
    maxMessages: Int, 
    waitMs: Long): Int

  /**
   * Sends final acknowledgments for a set of committed Spark batches.
   *
   * @param nativePtr Pointer to the native reader object.
   * @param batchIds  List of Spark batch IDs to acknowledge.
   * @return 1 on success, negative error code on failure.
   */
  @native def ackCommitted(nativePtr: Long, batchIds: java.util.List[String]): Int
  
  /**
   * Checks the count of messages currently in-flight (unacked).
   *
   * @param nativePtr Pointer to the native reader object.
   * @return The number of unacknowledged messages tracked by the native reader.
   */
  @native def getUnackedCount(nativePtr: Long): Int

  /**
   * Safely deallocates the native Rust object.
   *
   * @param nativePtr Pointer to the native reader object to close.
   */
  @native def close(nativePtr: Long): Unit

  // Metrics: Directly exported from Rust atomic counters for integration with Spark UI/MetricsSystem.
  
  /**
   * Gets the current native memory usage (buffered bytes).
   * @return Bytes currently held in the native buffer.
   */
  @native def getNativeMemoryUsageNative(): Long

  /**
   * Gets total ingested bytes since initialization.
   * @return Total bytes received from Pub/Sub.
   */
  @native def getIngestedBytesNative(): Long

  /**
   * Gets total ingested messages since initialization.
   * @return Total messages received from Pub/Sub.
   */
  @native def getIngestedMessagesNative(): Long

  /**
   * Gets total read errors encountered.
   * @return Count of read errors (e.g., gRPC failures).
   */
  @native def getReadErrorsNative(): Long

  /**
   * Gets total retry attempts made by the client.
   * @return Count of retries.
   */
  @native def getRetryCountNative(): Long

  /**
   * Gets the average acknowledgment latency in microseconds.
   * @return Average latency in micros.
   */
  @native def getAckLatencyMicrosNative(): Long
}
