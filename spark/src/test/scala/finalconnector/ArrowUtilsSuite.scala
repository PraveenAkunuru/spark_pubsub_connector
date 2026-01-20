package finalconnector

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.IntVector

class ArrowUtilsSuite extends AnyFunSuite {

  test("toArrowSchema converts basic Spark types") {
    val sparkSchema = StructType(Seq(
      StructField("col1", StringType, nullable = true),
      StructField("col2", IntegerType, nullable = false)
    ))

    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema)
    val fields = arrowSchema.getFields
    
    assert(fields.size() == 2)
    assert(fields.get(0).getName == "col1")
    assert(fields.get(0).getType.isInstanceOf[ArrowType.Utf8])
    assert(fields.get(0).isNullable)
    
    assert(fields.get(1).getName == "col2")
    assert(fields.get(1).getType.isInstanceOf[ArrowType.Int])
    assert(!fields.get(1).isNullable)
  }

  test("setValue and getValue handling for Integers") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new IntVector("intCol", allocator)
    try {
      vector.allocateNew(1)
      val row = new GenericInternalRow(Array[Any](42))
      
      ArrowUtils.setValue(vector, IntegerType, row, 0, 0)
      
      val value = ArrowUtils.getValue(vector, 0)
      assert(value == 42)
    } finally {
      vector.close()
      allocator.close()
    }
  }

  test("setValue and getValue handling for Strings") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarCharVector("stringCol", allocator)
    try {
      vector.allocateNew(1)
      val row = new GenericInternalRow(Array[Any](org.apache.spark.unsafe.types.UTF8String.fromString("hello")))
      
      ArrowUtils.setValue(vector, StringType, row, 0, 0)
      
      val value = ArrowUtils.getValue(vector, 0)
      assert(value.toString == "hello")
    } finally {
      vector.close()
      allocator.close()
    }
  }
}
