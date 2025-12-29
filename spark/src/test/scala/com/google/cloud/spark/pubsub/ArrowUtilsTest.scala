package com.google.cloud.spark.pubsub

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector._

class ArrowUtilsTest extends AnyFunSuite {

  test("ArrowUtils.toArrowSchema should map Spark types correctly") {
    val sparkSchema = StructType(Seq(
      StructField("binaryCol", BinaryType),
      StructField("stringCol", StringType),
      StructField("timestampCol", TimestampType),
      StructField("booleanCol", BooleanType),
      StructField("intCol", IntegerType),
      StructField("longCol", LongType),
      StructField("shortCol", ShortType),
      StructField("byteCol", ByteType),
      StructField("floatCol", FloatType),
      StructField("doubleCol", DoubleType)
    ))

    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema)
    val fields = arrowSchema.getFields
    assert(fields.size() == 10)
    assert(fields.get(0).getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Binary] != null)
    assert(fields.get(1).getType.asInstanceOf[org.apache.arrow.vector.types.pojo.ArrowType.Utf8] != null)
    // Add more specific checks if needed
  }

  test("ArrowUtils.setValue and getValue should work for all supported types") {
    val allocator = new RootAllocator()
    
    // Test Integer
    val intVector = new IntVector("intCol", allocator)
    intVector.allocateNew()
    val rowInt = InternalRow(123)
    ArrowUtils.setValue(intVector, IntegerType, rowInt, 0, 0)
    assert(intVector.get(0) == 123)
    assert(ArrowUtils.getValue(intVector, 0) == 123)
    
    // Test String
    val strVector = new VarCharVector("strCol", allocator)
    strVector.allocateNew()
    val rowStr = InternalRow(org.apache.spark.unsafe.types.UTF8String.fromString("hello"))
    ArrowUtils.setValue(strVector, StringType, rowStr, 0, 0)
    assert(new String(strVector.get(0)) == "hello")
    assert(ArrowUtils.getValue(strVector, 0).toString == "hello")
    
    // Test Short (mapped to IntVector in ArrowUtils)
    val shortVector = new IntVector("shortCol", allocator)
    shortVector.allocateNew()
    val rowShort = InternalRow(12345.toShort)
    ArrowUtils.setValue(shortVector, ShortType, rowShort, 0, 0)
    assert(shortVector.get(0) == 12345)
    assert(ArrowUtils.getValue(shortVector, 0) == 12345)

    intVector.close()
    strVector.close()
    shortVector.close()
    allocator.close()
  }
}
