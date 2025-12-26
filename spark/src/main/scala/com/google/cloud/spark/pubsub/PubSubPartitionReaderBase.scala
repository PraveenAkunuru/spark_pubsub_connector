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

  protected val nativePtr: Long = reader.init(
    partition.projectId, 
    partition.subscriptionId, 
    partition.jitterMillis, 
    schemaJson
  )

  if (nativePtr == 0) {
    throw new RuntimeException("Failed to initialize native Pub/Sub client.")
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
      
      if (result > 0) {
        // Import takes ownership of the C structs
        val root = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
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
