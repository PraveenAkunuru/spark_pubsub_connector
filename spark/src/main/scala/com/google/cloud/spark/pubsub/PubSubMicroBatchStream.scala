package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.{ColumnarBatch, ArrowColumnVector}
import scala.collection.JavaConverters._

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

  private val spark = SparkSession.active
  private val projectId = PubSubConfig.getOption(PubSubConfig.PROJECT_ID_KEY, options, spark).getOrElse("")
  private val subscriptionId = PubSubConfig.getOption(PubSubConfig.SUBSCRIPTION_ID_KEY, options, spark).getOrElse("")

  if (subscriptionId.isEmpty) {
    throw new IllegalArgumentException(s"Missing required option: '${PubSubConfig.SUBSCRIPTION_ID_KEY}'. Please provide a valid subscription ID.")
  }

  
  private var currentOffset: Long = 0
  // Map of BatchID -> Remaining Cycles (TTL). 
  // We keep a batch ID for a few cycles to ensure all executors have a chance to see it 
  // and flush their reservoirs. Default TTL is 5 cycles.
  private val pendingCommits = scala.collection.mutable.Map[String, Int]()
  private val ACK_TTL_CYCLES = 5

  override def initialOffset(): Offset = PubSubOffset(0)

  override def deserializeOffset(json: String): Offset = {
    PubSubOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {
    val batchId = end.json()
    logInfo(s"Batch $batchId committed by Spark. Signal will be propagated for next $ACK_TTL_CYCLES cycles.")
    pendingCommits.put(batchId, ACK_TTL_CYCLES)
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
    val defaultParallelism = spark.sparkContext.defaultParallelism

    val maxPartitions = defaultParallelism * 2
    val requestedPartitions = PubSubConfig.getOption(PubSubConfig.NUM_PARTITIONS_KEY, options, spark)
      .getOrElse(defaultParallelism.toString).toInt
    val numPartitions = if (requestedPartitions > maxPartitions) {
      logWarning(s"Reducing numPartitions from $requestedPartitions to $maxPartitions to stay within connection quotas (max 2x cores).")
      maxPartitions
    } else {
      requestedPartitions
    }

    // Decrement TTL for all pending commits and filter out expired ones
    val expiredBatches = scala.collection.mutable.ListBuffer[String]()
    pendingCommits.foreach { case (batchId, ttl) =>
      if (ttl <= 1) {
        expiredBatches += batchId
      } else {
        pendingCommits.update(batchId, ttl - 1)
      }
    }
    expiredBatches.foreach(pendingCommits.remove)
    
    val committedSignals = pendingCommits.keys.toList
    
    // Once prioritized for planning, we assume they will be acked by one of the tasks.
    // In a multi-executor environment, all executors on the cluster will receive this signal
    // and flush their local reservoirs for these batch IDs.
    
    
    val jitterMillis = PubSubConfig.getOption(PubSubConfig.JITTER_MS_KEY, options, spark)
      .getOrElse(PubSubConfig.DEFAULT_JITTER_MS).toInt
    val format = PubSubConfig.getOption(PubSubConfig.FORMAT_KEY, options, spark)
    val avroSchema = PubSubConfig.getOption(PubSubConfig.AVRO_SCHEMA_KEY, options, spark)

    (0 until numPartitions).map { i =>
      PubSubInputPartition(i, projectId, subscriptionId, committedSignals, end.json(), jitterMillis, format, avroSchema)
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
    batchId: String,
    jitterMillis: Int,
    format: Option[String],
    avroSchema: Option[String]) extends InputPartition

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
  extends PubSubPartitionReaderBase[InternalRow](partition, schema) {
  
  import com.google.cloud.spark.pubsub.ArrowUtils
  
  private var currentBatch: java.util.Iterator[InternalRow] = java.util.Collections.emptyIterator()
  private var currentVectorSchemaRoot: org.apache.arrow.vector.VectorSchemaRoot = _

  override def next(): Boolean = {
    if (currentBatch.hasNext) return true
    fetchNextBatch()
  }

  private def fetchNextBatch(): Boolean = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }

    fetchNativeBatch() match {
      case Some(root) =>
        currentVectorSchemaRoot = root
        val rowCount = root.getRowCount
        
        // Project to requested Schema
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
        
      case None =>
        false
    }
  }

  override def get(): InternalRow = currentBatch.next()

  override def close(): Unit = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }
    super.close()
  }
}

/**
 * Columnar version of the reader that stays in Arrow memory without intermediate heap conversions.
 */
class PubSubColumnarPartitionReader(partition: PubSubInputPartition, schema: StructType)
  extends PubSubPartitionReaderBase[ColumnarBatch](partition, schema) {

  private var currentBatch: ColumnarBatch = _
  private var currentVectorSchemaRoot: org.apache.arrow.vector.VectorSchemaRoot = _
  private var isExhausted = false

  override def next(): Boolean = {
    if (isExhausted) return false
    
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
      currentVectorSchemaRoot = null
    }

    fetchNativeBatch() match {
      case Some(root) =>
        currentVectorSchemaRoot = root
        val columnarVectors = schema.fields.map { field =>
          new ArrowColumnVector(root.getVector(field.name))
        }

        currentBatch = new ColumnarBatch(columnarVectors.toArray, root.getRowCount)
        true
        
      case None =>
        isExhausted = true
        false
    }
  }

  override def get(): ColumnarBatch = currentBatch

  override def close(): Unit = {
    if (currentVectorSchemaRoot != null) {
      currentVectorSchemaRoot.close()
    }
    super.close()
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
