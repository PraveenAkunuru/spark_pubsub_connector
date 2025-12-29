package com.google.cloud.spark.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VarBinaryVector}
import org.apache.arrow.c.{ArrowArray, ArrowSchema}

class NegativeReaderTest extends AnyFunSuite {

  test("NativeReader.init should fail with invalid configuration") {
    NativeReader.load()
    val reader = new NativeReader()
    // Invalid project/subscription
    val ptr = reader.init("", "", 0, "", 0)
    // Should return 0 (NULL pointer)
    assert(ptr == 0, "Reader init should return 0 for invalid config")
    
    if (ptr != 0) {
       reader.close(ptr)
    }
  }

  test("NativeReader.getNextBatch should handle invalid pointer safely") {
    NativeReader.load()
    val reader = new NativeReader()
    // Pass 0 as pointer
    val res = reader.getNextBatch(0, "batch-1", 0, 0, 10, 1000L)
    // Should return error code (e.g. -1 for invalid pointer)
    assert(res < 0, "Should return negative error code for invalid pointer")
  }
  
  test("NativeReader.close should handle invalid pointer safely") {
    NativeReader.load()
    val reader = new NativeReader()
    // Should not crash
    reader.close(0)
  }
}
