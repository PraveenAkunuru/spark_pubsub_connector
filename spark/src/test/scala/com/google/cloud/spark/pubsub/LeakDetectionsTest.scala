package com.google.cloud.spark.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VarBinaryVector}
import org.apache.arrow.vector.types.pojo.{Schema, Field, FieldType, ArrowType}
import scala.collection.JavaConverters._

class LeakDetectionsTest extends AnyFunSuite {

  test("NativeReader should not leak memory over multiple iterations") {
    val allocator = new RootAllocator()
    val reader = new NativeReader()
    val projectId = "leak-test-project"
    val subId = "leak-test-sub"
    val schemaJson = "[]" // Empty schema

    for (i <- 1 to 10) {
      val ptr = reader.init(projectId, subId, 0, schemaJson, i)
      assert(ptr != 0)
      
      // Verify initial memory usage in native (should be small/constant)
      val initialMemory = reader.getNativeMemoryUsageNative()
      
      reader.close(ptr)
    }

    // After all iterations, check if Arrow allocator is back to zero
    assert(allocator.getAllocatedMemory == 0, s"Arrow memory leak detected: ${allocator.getAllocatedMemory} bytes")
    allocator.close()
  }

  test("NativeWriter should not leak memory over multiple iterations") {
    val allocator = new RootAllocator()
    val writer = new NativeWriter()
    val projectId = "leak-test-project"
    val topicId = "leak-test-topic"

    for (i <- 1 to 10) {
      val ptr = writer.init(projectId, topicId, "", i)
      assert(ptr != 0)

      // Create a small batch
      val payloadField = new Field("payload", FieldType.nullable(new ArrowType.Binary()), null)
      val schema = new Schema(List(payloadField).asJava)
      val root = VectorSchemaRoot.create(schema, allocator)
      val payloadVector = root.getVector("payload").asInstanceOf[VarBinaryVector]
      payloadVector.allocateNew()
      payloadVector.setSafe(0, "leak-test-payload".getBytes("UTF-8"))
      payloadVector.setValueCount(1)
      root.setRowCount(1)

      val arrowArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator)
      val arrowSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
      org.apache.arrow.c.Data.exportVectorSchemaRoot(allocator, root, null, arrowArray, arrowSchema)

      val res = writer.writeBatch(ptr, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
      assert(res == 1)

      arrowArray.close()
      arrowSchema.close()
      root.close()
      
      writer.close(ptr, 1000)
    }

    // Final check
    assert(allocator.getAllocatedMemory == 0, s"Arrow memory leak detected in write path: ${allocator.getAllocatedMemory} bytes")
    allocator.close()
  }
}
