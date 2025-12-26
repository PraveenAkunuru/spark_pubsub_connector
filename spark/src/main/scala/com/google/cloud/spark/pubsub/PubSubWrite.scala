package com.google.cloud.spark.pubsub

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.JavaConverters._

/**
 * Builder for creating a `PubSubWrite` operation.
 */
class PubSubWriteBuilder(schema: StructType, options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def build(): Write = {
    val spark = org.apache.spark.sql.SparkSession.active
    // Resolve mandatory/optional options on the Driver
    val resolvedOptions = Map(
      PubSubConfig.PROJECT_ID_KEY -> PubSubConfig.getOption(PubSubConfig.PROJECT_ID_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse(""),
      PubSubConfig.TOPIC_ID_KEY -> PubSubConfig.getOption(PubSubConfig.TOPIC_ID_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse(""),
      PubSubConfig.BATCH_SIZE_KEY -> PubSubConfig.getOption(PubSubConfig.BATCH_SIZE_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse(PubSubConfig.DEFAULT_BATCH_SIZE.toString),
      PubSubConfig.LINGER_MS_KEY -> PubSubConfig.getOption(PubSubConfig.LINGER_MS_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse(PubSubConfig.DEFAULT_LINGER_MS.toString),
      PubSubConfig.MAX_BATCH_BYTES_KEY -> PubSubConfig.getOption(PubSubConfig.MAX_BATCH_BYTES_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse("5242880"),
      PubSubConfig.FLUSH_TIMEOUT_MS_KEY -> PubSubConfig.getOption(PubSubConfig.FLUSH_TIMEOUT_MS_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse(PubSubConfig.DEFAULT_FLUSH_TIMEOUT_MS.toString),
      PubSubConfig.FORMAT_KEY -> PubSubConfig.getOption(PubSubConfig.FORMAT_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse("json"),
      PubSubConfig.AVRO_SCHEMA_KEY -> PubSubConfig.getOption(PubSubConfig.AVRO_SCHEMA_KEY, options.asCaseSensitiveMap().asScala.toMap, spark).getOrElse("")
    )

    if (resolvedOptions(PubSubConfig.TOPIC_ID_KEY).isEmpty) {
      throw new IllegalArgumentException(s"Missing required option: '${PubSubConfig.TOPIC_ID_KEY}'. Please provide a valid topic ID.")
    }

    new PubSubWrite(schema, resolvedOptions)
  }
}

/**
 * Defines a structural write operation to Pub/Sub.
 */
class PubSubWrite(schema: StructType, options: Map[String, String]) extends Write {
  override def toBatch: BatchWrite = new PubSubBatchWrite(schema, options)
}

/**
 * Orchestrates batch writing across executors.
 */
class PubSubBatchWrite(schema: StructType, options: Map[String, String]) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    new PubSubDataWriterFactory(schema, options)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

/**
 * Factory class that initializes `PubSubDataWriter` on Spark executors.
 */
class PubSubDataWriterFactory(schema: StructType, options: Map[String, String]) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new PubSubDataWriter(partitionId, taskId, schema, options)
  }
}

/**
 * The core data writer that runs on Spark executors.
 *
 * It manages:
 * 1. **Buffering**: Accumulating Spark `InternalRow`s into an Arrow `VectorSchemaRoot`.
 * 2. **NativeWriter**: The JNI bridge to the Rust Publisher.
 * 3. **FFI Export**: Synchronously exporting Arrow batches to the C Data Interface for Rust to consume.
 */
class PubSubDataWriter(partitionId: Int, taskId: Long, schema: StructType, options: Map[String, String]) 
  extends DataWriter[InternalRow] with org.apache.spark.internal.Logging {
  
  private val projectId = options.getOrElse(PubSubConfig.PROJECT_ID_KEY, "")
  private val topicId = options.getOrElse(PubSubConfig.TOPIC_ID_KEY, "")
  private val batchSizeRaw = options.getOrElse(PubSubConfig.BATCH_SIZE_KEY, PubSubConfig.DEFAULT_BATCH_SIZE.toString).toInt
  private val batchSize = if (batchSizeRaw > 1000) {
    logWarning(s"Configured batchSize $batchSizeRaw exceeds Pub/Sub limit of 1000. Capping at 1000.")
    1000
  } else {
    batchSizeRaw
  }
  private val lingerMs = options.getOrElse(PubSubConfig.LINGER_MS_KEY, PubSubConfig.DEFAULT_LINGER_MS.toString).toLong
  
  logInfo(s"PubSubDataWriter initialized for $projectId/$topicId with batchSize: $batchSize, lingerMs: $lingerMs")
  private val maxBatchBytes = options.getOrElse(PubSubConfig.MAX_BATCH_BYTES_KEY, "5242880").toLong // 5MB default
  private val flushTimeoutMs = options.getOrElse(PubSubConfig.FLUSH_TIMEOUT_MS_KEY, PubSubConfig.DEFAULT_FLUSH_TIMEOUT_MS.toString).toLong
  
  private val writer = new NativeWriter()
  logInfo(s"PubSubDataWriter created for $projectId/$topicId, partitionId: $partitionId, taskId: $taskId")
  
  private val nativePtr = writer.init(projectId, topicId)
  if (nativePtr == 0) {
    throw new RuntimeException("Failed to initialize native Pub/Sub writer.")
  }

  // Arrow Allocator
  private val allocator = new org.apache.arrow.memory.RootAllocator()
  
  // Current active root for buffering
  private var root = org.apache.arrow.vector.VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema), allocator)
  private var vectors = schema.fields.map(f => root.getVector(f.name))
  private var rowCount = 0
  private var currentBatchBytes = 0L
  private var lastFlushTime = System.currentTimeMillis()

  override def write(record: InternalRow): Unit = {
    // Append row to vectors
    for (i <- schema.indices) {
      ArrowUtils.setValue(vectors(i), schema.fields(i).dataType, record, i, rowCount)
    }
    rowCount += 1
    
    // Estimate size (rough check)
    if (rowCount % 100 == 0) {
       currentBatchBytes = vectors.map(_.getBufferSize.toLong).sum
    }

    val now = System.currentTimeMillis()
    if (rowCount >= batchSize || currentBatchBytes >= maxBatchBytes || (now - lastFlushTime >= lingerMs)) {
      flush()
    }
  }

  /**
   * Converts the current Arrow buffer into a C-compatible format and invokes the native publisher.
   *
   * This method:
   * 1. Sets the row count on the VectorSchemaRoot.
   * 2. Exports the root to C Data Interface structs (ArrowArray, ArrowSchema).
   * 3. Calls `NativeWriter.writeBatch` via JNI.
   * 4. Resets the buffer for next batch.
   *
   * @throws RuntimeException if the native write fails.
   */
  private def flush(): Unit = {
    if (rowCount == 0) return
    
    root.setRowCount(rowCount)
    
    val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
    val arrowSchemaFFI = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
    
    // We hand off the current root to Rust. 
    // We MUST NOT close this root in Java; Rust will release it via the FFI callback.
    val rootToExport = root
    
    try {
      org.apache.arrow.c.Data.exportVectorSchemaRoot(allocator, rootToExport, null, arrowArray, arrowSchemaFFI)
      
      // Call native write
      val res = writer.writeBatch(nativePtr, arrowArray.memoryAddress(), arrowSchemaFFI.memoryAddress())
      if (res < 0) {
        throw new RuntimeException(s"Native writeBatch failed with code $res")
      }
    } finally {
      arrowArray.close()
      arrowSchemaFFI.close()
      // We MUST close the root here to release Java's local reference.
      // Rust (via Move semantics) will release the exported reference.
      // Total RefCount: 2 -> 0 (1 by Rust, 1 by Java).
      rootToExport.close()
    }
    
    // Create a NEW root for the next batch to ensure no shared state with Rust
    root = org.apache.arrow.vector.VectorSchemaRoot.create(ArrowUtils.toArrowSchema(schema), allocator)
    vectors = schema.fields.map(f => root.getVector(f.name))
    rowCount = 0
    currentBatchBytes = 0
    lastFlushTime = System.currentTimeMillis()
  }

  override def commit(): WriterCommitMessage = {
    flush()
    new PubSubWriterCommitMessage(partitionId, taskId)
  }

  override def abort(): Unit = {
    close()
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
      try {
        val res = writer.close(nativePtr, flushTimeoutMs)
        if (res < 0) {
          logError(s"NativeWriter.close failed with code $res")
          throw new RuntimeException(s"NativeWriter.close failed with code $res")
        }
      } finally {
        // ALWAYS set closed = true and release Arrow resources.
        // On the Rust side, NativeWriter.close always consumes the pointer (Box::from_raw),
        // so we MUST NOT call it again even if it failed (e.g. timeout).
        closed = true
        if (root != null) {
          root.close()
        }
        allocator.close()
        logInfo(s"PubSubDataWriter closed for partitionId: $partitionId")
      }
    }
  }
}

class PubSubWriterCommitMessage(partitionId: Int, taskId: Long) extends WriterCommitMessage
