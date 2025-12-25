package com.google.cloud.spark.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VarBinaryVector}
import org.apache.arrow.vector.types.pojo.{Schema, Field, FieldType, ArrowType}
import scala.collection.JavaConverters._

class NativeWriterIntegrationTest extends AnyFunSuite {

  test("NativeWriter should init, write batch, and close without leaks") {
    // Ensure native library is loaded
    NativeReader.load()

    val projectId = "spark-test-project"
    val topicId = "test-topic"

    val writer = new NativeWriter()
    val ptr = writer.init(projectId, topicId)
    assert(ptr != 0, "NativeWriter init failed")

    // Create Arrow Data
    val allocator = new RootAllocator()
    val payloadField = new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)
    val schema = new Schema(List(payloadField).asJava)
    val root = VectorSchemaRoot.create(schema, allocator)
    
    val payloadVector = root.getVector("payload").asInstanceOf[VarBinaryVector]
    payloadVector.allocateNew()
    payloadVector.setSafe(0, "test-message-payload".getBytes("UTF-8"))
    payloadVector.setValueCount(1)
    root.setRowCount(1)

    // Export to C Data Interface
    val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
    val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
    org.apache.arrow.c.Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema)

    println(s"Scala: Allocator before writeBatch: ${allocator.getAllocatedMemory}")

    // Call Native Write (Move Semantics)
    // Rust takes ownership of the data in arrowArray/arrowSchema and will call release callback.
    val res = writer.writeBatch(ptr, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
    assert(res == 1, s"NativeWriter writeBatch failed with code $res")

    println(s"Scala: Allocator after writeBatch: ${allocator.getAllocatedMemory}")

    // Clean up
    // We close the wrappers for C structs
    arrowArray.close()
    println(s"Scala: Allocator after arrowArray.close(): ${allocator.getAllocatedMemory}")
    
    arrowSchema.close()
    println(s"Scala: Allocator after arrowSchema.close(): ${allocator.getAllocatedMemory}")
    
    // We do NOT close 'root' here because ownership was transferred/shared? 
    // Wait, if it's Move, Rust frees the buffers. 'root' in Java is just a view.
    // If we close 'root', it might try to free buffers again?
    // Let's verify allocator state.
    
    writer.close(ptr, 30000)
    
    // In Move semantics, Rust releases its reference (1 -> 0).
    // Java still holds 'root' (Ref 1). We MUST close it to release local reference.
    root.close()
    
    allocator.close()
  }
}
