package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.JavaConverters._

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

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    logDebug(s"planInputPartitions called with start=$start, end=$end")
    val projectId = options.getOrElse(PubSubConfig.PROJECT_ID_KEY, "")
    val subscriptionId = options.getOrElse(PubSubConfig.SUBSCRIPTION_ID_KEY, "")
    Array(PubSubInputPartition(0, projectId, subscriptionId))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new PubSubPartitionReaderFactory(schema)
  }
}

case class PubSubOffset(offset: Long) extends Offset {
  override def json(): String = offset.toString
}

case class PubSubInputPartition(partitionId: Int, projectId: String, subscriptionId: String) extends InputPartition

class PubSubPartitionReaderFactory(schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new PubSubPartitionReader(partition.asInstanceOf[PubSubInputPartition], schema)
  }
}

class PubSubPartitionReader(partition: PubSubInputPartition, schema: StructType) 
  extends PartitionReader[InternalRow] with org.apache.spark.internal.Logging {
  
  private val reader = new NativeReader()
  logInfo(s"PubSubPartitionReader created for ${partition.subscriptionId}")
  
  private val nativePtr = reader.init(partition.projectId, partition.subscriptionId)
  if (nativePtr == 0) {
    throw new RuntimeException("Failed to initialize native Pub/Sub client.")
  }
  
  private val allocator = new org.apache.arrow.memory.RootAllocator()
  private val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
  private val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
  
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

    val result = reader.getNextBatch(nativePtr, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
    
    if (result != 0) {
      val root = org.apache.arrow.c.Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
      currentVectorSchemaRoot = root
      
      val rowCount = root.getRowCount
      val vectors = root.getFieldVectors.asScala
      
      val rows = (0 until rowCount).map { i =>
        val values = vectors.indices.map { j =>
          ArrowUtils.getValue(vectors(j), i)
        }
        InternalRow.fromSeq(values)
      }.toIterator.asJava
      
      currentBatch = rows
      currentBatch.hasNext
    } else {
      false
    }
  }

  override def get(): InternalRow = currentBatch.next()

  override def close(): Unit = {
    if (currentVectorSchemaRoot != null) currentVectorSchemaRoot.close()
    arrowArray.close()
    arrowSchema.close()
    allocator.close()
    reader.close(nativePtr)
    logInfo(s"PubSubPartitionReader closed for ${partition.subscriptionId}")
  }
}
