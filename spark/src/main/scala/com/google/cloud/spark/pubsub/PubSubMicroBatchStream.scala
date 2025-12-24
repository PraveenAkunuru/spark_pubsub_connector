package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.JavaConverters._

/**
 * Orchestrates the Micro-Batch stream for Pub/Sub.
 *
 * This class handles:
 * 1. **Offset Management**: Tracking logical progress of the stream.
 * 2. **Partition Planning**: Splitting the workload into parallel tasks (partitions).
 * 3. **Reader Factory**: Creating executors-side readers.
 */
class PubSubMicroBatchStream(schema: StructType, options: Map[String, String]) 
  extends MicroBatchStream with org.apache.spark.internal.Logging {
  
  private var currentOffset: Long = 0

  override def initialOffset(): Offset = PubSubOffset(0)

  override def deserializeOffset(json: String): Offset = {
    PubSubOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def latestOffset(): Offset = {
    currentOffset += 1
    PubSubOffset(currentOffset)
  }

  /**
   * Plans the parallel workload by creating `numPartitions` input partitions.
   * Each partition will be handled by a separate executor task.
   */
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    logDebug(s"planInputPartitions called with start=$start, end=$end")
    val projectId = options.getOrElse(PubSubConfig.PROJECT_ID_KEY, "")
    val subscriptionId = options.getOrElse(PubSubConfig.SUBSCRIPTION_ID_KEY, "")
    val numPartitions = options.getOrElse(PubSubConfig.NUM_PARTITIONS_KEY, PubSubConfig.NUM_PARTITIONS_DEFAULT).toInt
    
    (0 until numPartitions).map { i =>
      PubSubInputPartition(i, projectId, subscriptionId)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PubSubPartitionReaderFactory(schema)
  }
}

/**
 * Simple Long-based offset for Pub/Sub.
 */
case class PubSubOffset(offset: Long) extends Offset {
  override def json(): String = offset.toString
}

/**
 * Metadata for a single parallel Spark task reading from Pub/Sub.
 */
case class PubSubInputPartition(partitionId: Int, projectId: String, subscriptionId: String) extends InputPartition

/**
 * Factory class that initializes `PubSubPartitionReader` on the executors.
 */
class PubSubPartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PubSubPartitionReader(partition.asInstanceOf[PubSubInputPartition], schema)
  }
}

/**
 * The core reader that runs on Spark executors.
 *
 * It manages the lifecycle of:
 * 1. **NativeReader**: The JNI bridge to Rust.
 * 2. **Arrow Allocator**: Memory management for zero-copy data transfer.
 * 3. **Acknowledgment Buffer**: Tracking message IDs for At-Least-Once delivery.
 */
class PubSubPartitionReader(partition: PubSubInputPartition, schema: StructType) 
  extends PartitionReader[InternalRow] with org.apache.spark.internal.Logging {
  
  private val reader = new NativeReader()
  logInfo(s"PubSubPartitionReader created for ${partition.subscriptionId}")
  
  private val nativePtr = reader.init(partition.projectId, partition.subscriptionId)
  if (nativePtr == 0) {
    throw new RuntimeException("Failed to initialize native Pub/Sub client.")
  }
  
  private val allocator = new org.apache.arrow.memory.RootAllocator()
  
  private var currentBatch: java.util.Iterator[InternalRow] = java.util.Collections.emptyIterator()
  private var currentVectorSchemaRoot: org.apache.arrow.vector.VectorSchemaRoot = _
  
  // Accumulate ack IDs for the current task
  private val ackIdBuffer = new scala.collection.mutable.ArrayBuffer[String]()

  /**
   * Advances the reader to the next row. Fetches a new batch from native if the current batch is exhausted.
   */
  override def next(): Boolean = {
    if (currentBatch.hasNext) return true
    fetchNextBatch()
  }

  private def fetchNextBatch(): Boolean = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }

    // Allocate Arrow structures fresh per batch as they are owned by VectorSchemaRoot after import
    val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
    val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
    
    try {
      val result = reader.getNextBatch(nativePtr, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
      
      if (result != 0) {
        val root = org.apache.arrow.c.Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
        currentVectorSchemaRoot = root
        
        val rowCount = root.getRowCount
        
        // 1. Extract Ack IDs
        val ackIdVector = root.getVector("ack_id")
        if (ackIdVector != null) {
          for (i <- 0 until rowCount) {
            val ackIdRef = ArrowUtils.getValue(ackIdVector, i)
            if (ackIdRef != null) {
               ackIdBuffer += ackIdRef.toString
            }
          }
        }
        
        // 2. Project to requested Schema
        val fieldVectors = schema.fields.map { field =>
          root.getVector(field.name)
        }
        
        val rows = (0 until rowCount).map { i =>
          val values = fieldVectors.indices.map { j =>
            val vec = fieldVectors(j)
            if (vec != null) {
               ArrowUtils.getValue(vec, i)
            } else {
               null
            }
          }
          InternalRow.fromSeq(values)
        }
        
        currentBatch = rows.toIterator.asJava
        currentBatch.hasNext
      } else {
        arrowArray.close()
        arrowSchema.close()
        false
      }
    } catch {
      case e: Exception =>
        arrowArray.close()
        arrowSchema.close()
        throw e
    }
  }

  override def get(): InternalRow = currentBatch.next()

  /**
   * Finalizes the task. Acknowledges all processed messages in Pub/Sub and releases memory.
   */
  override def close(): Unit = {
    // Ack all messages processed in this task
    if (ackIdBuffer.nonEmpty) {
      logInfo(s"Acknowledging ${ackIdBuffer.size} messages for ${partition.subscriptionId}")
      reader.acknowledge(nativePtr, ackIdBuffer.asJava)
      ackIdBuffer.clear()
    }

    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }
    reader.close(nativePtr)
    allocator.close()
  }
}
