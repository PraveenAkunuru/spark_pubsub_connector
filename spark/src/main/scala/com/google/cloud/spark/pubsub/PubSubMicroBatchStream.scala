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

  logInfo(s"PubSubMicroBatchStream initialized: projectId=$projectId, subscriptionId=$subscriptionId")

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
    
    logInfo(s"Options: $options")
    val requestedPartitions = options.get(PubSubConfig.NUM_PARTITIONS_KEY).map(_.toInt)
    logInfo(s"Requested Partitions: $requestedPartitions")
    
    val numPartitions = requestedPartitions.getOrElse {
      val conf = spark.sparkContext.getConf
      
      // Diagnostic logging to understand environment
      logInfo("Spark Configuration for Partitioning:")
      conf.getAll.filter(p => 
        p._1.contains("executor") || p._1.contains("dynamicAllocation") || p._1.contains("master")
      ).foreach(p => logInfo(s"  ${p._1} = ${p._2}"))

      val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt).getOrElse(1)
      val numExecutors = conf.getOption("spark.executor.instances").map(_.toInt)
        .orElse(conf.getOption("spark.dynamicAllocation.maxExecutors").map(_.toInt))
        .getOrElse(Math.max(1, spark.sparkContext.defaultParallelism / coresPerExecutor))
      
      val totalCores = numExecutors * coresPerExecutor
      // Use the max of config-based cores and current active cores
      val cores = Math.max(totalCores, spark.sparkContext.defaultParallelism)
      
      val expectedMbS = PubSubConfig.getOption(PubSubConfig.EXPECTED_THROUGHPUT_MB_S_KEY, options, spark)
        .getOrElse(PubSubConfig.DEFAULT_EXPECTED_THROUGHPUT).toInt
      
      val tFloor = Math.ceil(expectedMbS / 8.0).toInt
      val pHeadroom = cores * 3
      
      val base = Math.max(tFloor, pHeadroom)
      val hcn = findNextHighlyCompositeNumber(base)
      
      logInfo(s"Intelligent Partitioning: cores=$cores (executors=$numExecutors, coresPerExec=$coresPerExecutor), expectedMbS=$expectedMbS => base=$base, hcn=$hcn")
      hcn
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
    logInfo(s"Planning $numPartitions input partitions")

    val committedSignals = pendingCommits.keys.toList
    
    val jitterMillis = PubSubConfig.getOption(PubSubConfig.JITTER_MS_KEY, options, spark)
      .getOrElse(PubSubConfig.DEFAULT_JITTER_MS).toInt
    val format = PubSubConfig.getOption(PubSubConfig.FORMAT_KEY, options, spark)
    val avroSchema = PubSubConfig.getOption(PubSubConfig.AVRO_SCHEMA_KEY, options, spark)
    val caCertificatePath = PubSubConfig.getOption(PubSubConfig.CA_CERTIFICATE_PATH_KEY, options, spark)
    val readBatchSize = PubSubConfig.getOption(PubSubConfig.BATCH_SIZE_KEY, options, spark)
      .getOrElse(PubSubConfig.DEFAULT_BATCH_SIZE.toString).toInt
    val readWaitMs = PubSubConfig.getOption(PubSubConfig.READ_WAIT_MS_KEY, options, spark)
      .getOrElse(PubSubConfig.DEFAULT_READ_WAIT_MS).toLong

    (0 until numPartitions).map { i =>
      PubSubInputPartition(i, projectId, subscriptionId, committedSignals, end.json(), jitterMillis, format, avroSchema, caCertificatePath, readBatchSize, readWaitMs)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PubSubPartitionReaderFactory(schema)
  }

  /**
   * Returns the smallest Highly Composite Number greater than or equal to n.
   * HCNs are numbers with more divisors than any smaller positive integer.
   * They are ideal for Spark because they can be divided evenly into many smaller sizes.
   */
  private def findNextHighlyCompositeNumber(n: Int): Int = {
    val hcns = Array(
      1, 2, 4, 6, 12, 24, 36, 48, 60, 120, 180, 240, 360, 720, 840, 1260, 1680, 2520, 5040, 7560, 10080, 15120, 20160, 25200, 27720, 45360, 50400, 55440, 83160, 110880
    )
    hcns.find(_ >= n).getOrElse(n)
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
    avroSchema: Option[String],
    caCertificatePath: Option[String],
    batchSize: Int,
    readWaitMs: Long) extends InputPartition

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


