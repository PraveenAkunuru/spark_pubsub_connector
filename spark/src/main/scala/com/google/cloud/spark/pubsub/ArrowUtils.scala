package com.google.cloud.spark.pubsub

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

object ArrowUtils {
  def toArrowSchema(sparkSchema: StructType): Schema = {
    val fields = sparkSchema.fields.map { f =>
      val arrowType = f.dataType match {
        case BinaryType => new ArrowType.Binary()
        case StringType => new ArrowType.Utf8()
        case TimestampType => new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")
        case BooleanType => new ArrowType.Bool()
        case IntegerType => new ArrowType.Int(32, true)
        case LongType => new ArrowType.Int(64, true)
        case ShortType => new ArrowType.Int(32, true)
        case ByteType => new ArrowType.Int(32, true)
        case FloatType => new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)
        case DoubleType => new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)
        case _ => throw new UnsupportedOperationException(s"Unsupported type: ${f.dataType}")
      }
      val fieldType = FieldType.nullable(arrowType)
      new Field(f.name, fieldType, null)
    }
    new Schema(fields.toList.asJava)
  }

  def setValue(v: org.apache.arrow.vector.FieldVector, dataType: DataType, record: org.apache.spark.sql.catalyst.InternalRow, pos: Int, rowCount: Int): Unit = {
    import org.apache.arrow.vector._
    if (record.isNullAt(pos)) {
      v.setNull(rowCount)
    } else {
      dataType match {
        case BinaryType => v.asInstanceOf[VarBinaryVector].setSafe(rowCount, record.getBinary(pos))
        case StringType => v.asInstanceOf[VarCharVector].setSafe(rowCount, record.getUTF8String(pos).getBytes)
        case TimestampType => v.asInstanceOf[TimeStampMicroTZVector].setSafe(rowCount, record.getLong(pos))
        case BooleanType => v.asInstanceOf[BitVector].setSafe(rowCount, if (record.getBoolean(pos)) 1 else 0)
        case IntegerType => v.asInstanceOf[IntVector].setSafe(rowCount, record.getInt(pos))
        case LongType => v.asInstanceOf[BigIntVector].setSafe(rowCount, record.getLong(pos))
        case ShortType => v.asInstanceOf[IntVector].setSafe(rowCount, record.getShort(pos).toInt)
        case ByteType => v.asInstanceOf[IntVector].setSafe(rowCount, record.getByte(pos).toInt)
        case FloatType => v.asInstanceOf[Float4Vector].setSafe(rowCount, record.getFloat(pos))
        case DoubleType => v.asInstanceOf[Float8Vector].setSafe(rowCount, record.getDouble(pos))
        case _ => v.setNull(rowCount)
      }
    }
  }

  def getValue(v: org.apache.arrow.vector.FieldVector, pos: Int): Any = {
    import org.apache.arrow.vector._
    if (v.isNull(pos)) return null
    
    v match {
      case varChar: VarCharVector =>
        org.apache.spark.unsafe.types.UTF8String.fromBytes(varChar.get(pos))
      case binary: VarBinaryVector =>
        binary.get(pos)
      case ts: TimeStampMicroTZVector =>
        ts.get(pos)
      case bit: BitVector =>
        bit.get(pos) != 0
      case intV: IntVector =>
        // For ShortType and ByteType, which are mapped to IntVector,
        // we return an Int. Spark's InternalRow can handle this.
        intV.get(pos)
      case longV: BigIntVector =>
        longV.get(pos)
      case floatV: Float4Vector =>
        floatV.get(pos)
      case doubleV: Float8Vector =>
        doubleV.get(pos)
      case _ => v.getObject(pos)
    }
  }
}
