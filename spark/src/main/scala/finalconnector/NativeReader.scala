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
   */
  @native def ackCommitted(nativePtr: Long, batchIds: java.util.List[String]): Int
  
  /**
   * Checks the count of messages currently in-flight (unacked).
   */
  @native def getUnackedCount(nativePtr: Long): Int

  /**
   * Safely deallocates the native Rust object.
   */
  @native def close(nativePtr: Long): Unit

  // Metrics: Directly exported from Rust atomic counters for integration with Spark UI/MetricsSystem.
  @native def getNativeMemoryUsageNative(): Long
  @native def getIngestedBytesNative(): Long
  @native def getIngestedMessagesNative(): Long
  @native def getReadErrorsNative(): Long
  @native def getRetryCountNative(): Long
  @native def getAckLatencyMicrosNative(): Long
}
