package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import scala.collection.JavaConverters._

/**
 * Base class for Pub/Sub partition readers, handling common JNI lifecycle and memory management.
 */
abstract class PubSubPartitionReaderBase[T](
    partition: PubSubInputPartition, 
    schema: StructType) 
  extends PartitionReader[T] with Logging {

  protected val reader = new NativeReader()
  logInfo(s"PubSubPartitionReader created for ${partition.subscriptionId}")

  /** JSON-serialized configuration for the native data plane. */
  protected val schemaJson: String = PubSubConfig.buildProcessingConfigJson(
    schema, 
    partition.format, 
    partition.avroSchema,
    partition.caCertificatePath
  )

  System.err.println(s"DEBUG EXECUTOR: Initializing partition ${partition.partitionId} (Batch: ${partition.batchId})")
  System.err.println(s"DEBUG EXECUTOR: JNI Init parameters -> projectId='${partition.projectId}', subscriptionId='${partition.subscriptionId}'")

  protected val nativePtr: Long = try {
    reader.init(
      partition.projectId, 
      partition.subscriptionId, 
      partition.jitterMillis, 
      schemaJson
    )
  } catch {
    case e: Throwable =>
      logError(s"Fatal error during NativeReader.init: ${e.getMessage}", e)
      throw new RuntimeException(s"DEBUG_FATAL: proj='${partition.projectId}', sub='${partition.subscriptionId}', err=${e.getMessage}", e)
  }

  if (nativePtr == 0) {
    throw new RuntimeException(s"DEBUG_PTR_ZERO: proj='${partition.projectId}', sub='${partition.subscriptionId}'")
  }
  protected val allocator = new RootAllocator()

  // Handle committed acks
  if (partition.committedBatchIds.nonEmpty) {
    logInfo(s"Task: Propagation signal received. Flushing committed batches: ${partition.committedBatchIds.mkString(", ")}")
    reader.ackCommitted(nativePtr, partition.committedBatchIds.asJava)
  }

  /**
   * Fetches the next batch from the native layer.
   * 
   * @return Option containing the VectorSchemaRoot if a batch was read, or None if raw queue is empty or closed.
   *         The caller is responsible for maintaining ownership or closing the returned root.
   */
  protected def fetchNativeBatch(): Option[VectorSchemaRoot] = {
    val arrowArray = ArrowArray.allocateNew(allocator)
    val arrowSchema = ArrowSchema.allocateNew(allocator)

    try {
      val result = reader.getNextBatch(nativePtr, partition.batchId, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
      logDebug(s"fetchNativeBatch: batchId=${partition.batchId}, partition=${partition.partitionId}, result=$result")
      
      if (result > 0) {
        // Import takes ownership of the C structs
        val root = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
        logInfo(s"Successfully fetched batch with ${root.getRowCount} rows on partition ${partition.partitionId}")
        Some(root)
      } else if (result == 0) {
        arrowArray.close()
        arrowSchema.close()
        None
      } else {
        arrowArray.close()
        arrowSchema.close()
        throw new RuntimeException(s"NativeReader.getNextBatch failed with code $result")
      }
    } catch {
      case e: Exception =>
        arrowArray.close()
        arrowSchema.close()
        throw e
    }
  }

  // Safety Net: Ensure close is called even if task fails
  Option(org.apache.spark.TaskContext.get()).foreach { tc =>
    tc.addTaskCompletionListener[Unit] { _ =>
      close()
    }
  }

  private var closed = false

  override def close(): Unit = {
    if (!closed) {
      reader.close(nativePtr)
      allocator.close()
      closed = true
    }
  }
}
