package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ColumnarBatch, ArrowColumnVector}
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
 * Orchestrates the Micro-Batch stream for Pub/Sub.
 *
 * This class handles:
 * 1. **Offset Management**: Tracking logical progress of the stream.
 * 2. **Partition Planning**: Splitting the workload into parallel tasks (partitions).
 * 3. **Reader Factory**: Creating executors-side readers.
 * 4. **Driver-Coordinated Acks**: Aggregating ack_ids from across the cluster and flushing on commit.
 */
class PubSubMicroBatchStream(schema: StructType, options: Map[String, String], checkpointLocation: String) 
  extends MicroBatchStream with org.apache.spark.internal.Logging {

  private val projectId = options.getOrElse(PubSubConfig.PROJECT_ID_KEY, "")
  private val subscriptionId = options.getOrElse(PubSubConfig.SUBSCRIPTION_ID_KEY, "")

  
  private var currentOffset: Long = 0
  private val pendingCommits = new scala.collection.mutable.HashSet[String]()

  override def initialOffset(): Offset = PubSubOffset(0)

  override def deserializeOffset(json: String): Offset = {
    PubSubOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {
    val batchId = end.json()
    logInfo(s"Batch $batchId committed by Spark. Signal will be propagated in the next planning cycle.")
    pendingCommits.add(batchId)
  }

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
    val defaultParallelism = SparkSession.active.sparkContext.defaultParallelism

    val maxPartitions = defaultParallelism * 2
    val requestedPartitions = options.getOrElse(PubSubConfig.NUM_PARTITIONS_KEY, PubSubConfig.NUM_PARTITIONS_DEFAULT).toInt
    val numPartitions = if (requestedPartitions > maxPartitions) {
      logWarning(s"Reducing numPartitions from $requestedPartitions to $maxPartitions to stay within connection quotas (max 2x cores).")
      maxPartitions
    } else {
      requestedPartitions
    }

    val committedSignals = pendingCommits.toList
    // Once prioritized for planning, we assume they will be acked by one of the tasks.
    // In a multi-executor environment, all executors on the cluster will receive this signal
    // and flush their local reservoirs for these batch IDs.
    
    (0 until numPartitions).map { i =>
      PubSubInputPartition(i, projectId, subscriptionId, committedSignals, end.json())
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
case class PubSubInputPartition(
    partitionId: Int, 
    projectId: String, 
    subscriptionId: String,
    committedBatchIds: List[String],
    batchId: String) extends InputPartition

/**
 * Factory class that initializes `PubSubPartitionReader` on the executors.
 */
class PubSubPartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PubSubPartitionReader(partition.asInstanceOf[PubSubInputPartition], schema)
  }

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    new PubSubColumnarPartitionReader(partition.asInstanceOf[PubSubInputPartition], schema)
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
  
  import scala.collection.JavaConverters._
  private var currentBatch: java.util.Iterator[InternalRow] = java.util.Collections.emptyIterator()
  private var currentVectorSchemaRoot: org.apache.arrow.vector.VectorSchemaRoot = _

  /**
   * Advances the reader to the next row. Fetches a new batch from native if the current batch is exhausted.
   */
  // Perform committed Acks from signal propagation
  if (partition.committedBatchIds.nonEmpty) {
    logInfo(s"Task: Propagation signal received. Flushing committed batches: ${partition.committedBatchIds.mkString(", ")}")
    reader.ackCommitted(nativePtr, partition.committedBatchIds.asJava)
  }

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
      val result = reader.getNextBatch(nativePtr, partition.batchId, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
      
      if (result != 0) {
        val root = org.apache.arrow.c.Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
        currentVectorSchemaRoot = root
        
        val rowCount = root.getRowCount
        
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
   * Finalizes the task.
   */
  override def close(): Unit = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }
    reader.close(nativePtr)
    allocator.close()
  }
}

/**
 * Columnar version of the reader that stays in Arrow memory without intermediate heap conversions.
 */
class PubSubColumnarPartitionReader(partition: PubSubInputPartition, schema: StructType)
  extends PartitionReader[ColumnarBatch] with org.apache.spark.internal.Logging {

  private val reader = new NativeReader()
  private val nativePtr = reader.init(partition.projectId, partition.subscriptionId)
  private val allocator = new org.apache.arrow.memory.RootAllocator()
  private var currentBatch: ColumnarBatch = _
  private var currentVectorSchemaRoot: org.apache.arrow.vector.VectorSchemaRoot = _
  private var isExhausted = false

  if (partition.committedBatchIds.nonEmpty) {
    import scala.collection.JavaConverters._
    reader.ackCommitted(nativePtr, partition.committedBatchIds.asJava)
  }

  override def next(): Boolean = {
    if (isExhausted) return false
    
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }

    val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
    val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
    
    try {
      val result = reader.getNextBatch(nativePtr, partition.batchId, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
      if (result != 0) {
        val root = org.apache.arrow.c.Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
        currentVectorSchemaRoot = root
        
        val columnarVectors = schema.fields.map { field =>
          new ArrowColumnVector(root.getVector(field.name))
        }
        currentBatch = new ColumnarBatch(columnarVectors.toArray, root.getRowCount)
        true
      } else {
        arrowArray.close()
        arrowSchema.close()
        isExhausted = true
        false
      }
    } catch {
      case e: Exception =>
        arrowArray.close()
        arrowSchema.close()
        throw e
    }
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
    }
    reader.close(nativePtr)
    allocator.close()
  }
}

/**
 * Custom metrics for Pub/Sub.
 */
class PubSubCustomMetric(metricName: String, metricDescription: String) 
  extends org.apache.spark.sql.connector.metric.CustomMetric {
  override def name(): String = metricName
  override def description(): String = metricDescription
  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = taskMetrics.sum.toString
}

case class PubSubCustomTaskMetric(metricName: String, metricValue: Long) 
  extends org.apache.spark.sql.connector.metric.CustomTaskMetric {
  override def name(): String = metricName
  override def value(): Long = metricValue
}
