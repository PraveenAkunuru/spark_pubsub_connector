package finalconnector

import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.arrow.vector._
import java.util.Collections
import scala.collection.JavaConverters._

/**
 * Utility for high-performance conversion between Spark SQL schemas/rows and Apache Arrow schemas/vectors.
 *
 * This object manages the type mapping between the Spark JVM and the Arrow memory format.
 * It is used by both the `NativeReader` (for data ingestion) and `NativeWriter` (for 
 * data publishing) to ensure binary compatibility across the JNI boundary.
 */
object ArrowUtils {

  /**
   * Converts a Spark StructType to an Arrow Schema.
   */
  def toArrowSchema(schema: StructType): Schema = {
    val fields = schema.fields.map { f =>
      val arrowType = f.dataType match {
        case StringType => new ArrowType.Utf8()
        case BinaryType => new ArrowType.Binary()
        case BooleanType => new ArrowType.Bool()
        case ByteType => new ArrowType.Int(8, true)
        case ShortType => new ArrowType.Int(16, true)
        case IntegerType => new ArrowType.Int(32, true)
        case LongType => new ArrowType.Int(64, true)
        case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        case TimestampType => new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")
        case DateType => new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)
        case _ => throw new UnsupportedOperationException(s"Unsupported Spark type: ${f.dataType}")
      }
      new Field(f.name, new FieldType(f.nullable, arrowType, null), Collections.emptyList[Field]())
    }
    new Schema(fields.toList.asJava)
  }

  /**
   * Sets a value in an Arrow FieldVector from a Spark InternalRow.
   */
  def setValue(vector: FieldVector, dataType: DataType, row: InternalRow, ordinal: Int, rowCount: Int): Unit = {
    if (row.isNullAt(ordinal)) {
      vector match {
        case v: BaseFixedWidthVector => v.setNull(rowCount)
        case v: BaseVariableWidthVector => v.setNull(rowCount)
        case _ => // handle others if needed
      }
      return
    }

    dataType match {
      case StringType =>
        vector.asInstanceOf[VarCharVector].setSafe(rowCount, row.getUTF8String(ordinal).getBytes)
      case BinaryType =>
        vector.asInstanceOf[VarBinaryVector].setSafe(rowCount, row.getBinary(ordinal))
      case BooleanType =>
        vector.asInstanceOf[BitVector].setSafe(rowCount, if (row.getBoolean(ordinal)) 1 else 0)
      case ByteType =>
        vector.asInstanceOf[TinyIntVector].setSafe(rowCount, row.getByte(ordinal))
      case ShortType =>
        vector.asInstanceOf[SmallIntVector].setSafe(rowCount, row.getShort(ordinal))
      case IntegerType =>
        vector.asInstanceOf[IntVector].setSafe(rowCount, row.getInt(ordinal))
      case LongType =>
        vector.asInstanceOf[BigIntVector].setSafe(rowCount, row.getLong(ordinal))
      case FloatType =>
        vector.asInstanceOf[Float4Vector].setSafe(rowCount, row.getFloat(ordinal))
      case DoubleType =>
        vector.asInstanceOf[Float8Vector].setSafe(rowCount, row.getDouble(ordinal))
      case TimestampType =>
        vector.asInstanceOf[TimeStampMicroTZVector].setSafe(rowCount, row.getLong(ordinal))
      case DateType =>
        vector.asInstanceOf[DateDayVector].setSafe(rowCount, row.getInt(ordinal))
      case _ => throw new UnsupportedOperationException(s"Unsupported Spark type: $dataType")
    }
  }

  /**
   * Gets a value from an Arrow FieldVector.
   */
  def getValue(vector: FieldVector, rowId: Int): Any = {
    if (vector.isNull(rowId)) return null
    
    vector match {
      case v: VarCharVector => new String(v.get(rowId), java.nio.charset.StandardCharsets.UTF_8)
      case v: VarBinaryVector => v.get(rowId)
      case v: BitVector => v.get(rowId) == 1
      case v: TinyIntVector => v.get(rowId)
      case v: SmallIntVector => v.get(rowId)
      case v: IntVector => v.get(rowId)
      case v: BigIntVector => v.get(rowId)
      case v: Float4Vector => v.get(rowId)
      case v: Float8Vector => v.get(rowId)
      case v: TimeStampMicroTZVector => v.get(rowId)
      case v: DateDayVector => v.get(rowId)
      case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vector.getClass}")
    }
  }
}
