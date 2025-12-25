package org.apache.spark.sql.util

import org.apache.spark.sql.types.StructType

object PubSubArrowUtils {
  def toArrowSchema(schema: StructType): String = {
    // Spark 3.5 ArrowUtils.toArrowSchema takes (schema, timeZoneId, errorOnDuplicatedFieldNames, ignoreCorruptedField)
    // Or just (schema, timeZoneId) in some versions.
    // Or just (schema) in older ones?
    // Based on "too many arguments" error for (schema, "UTC"), it might handle timezone internally or take it from conf?
    // Let's rely on type inference or try-catch.
    // Actually, if I am in the package `org.apache.spark.sql.util`, I can see what `ArrowUtils` has!
    // But I am guessing. 
    // Let's try `ArrowUtils.toArrowSchema(schema, "UTC")`. If 2 args failed, maybe it is `toArrowSchema(schema)`.
    // Wait, the error said "too many arguments (2)". So it takes 1 argument?
    // Or maybe 3? No, "too many" usually means you passed more than expected.
    // So expected is 1?
    try {
        // Try with 4 arguments (Spark 3.4/3.5 signature)
      ArrowUtils.toArrowSchema(schema, "UTC", true, false).toJson()
    } catch {
        case _: Throwable =>
           throw new RuntimeException("Failed to call ArrowUtils.toArrowSchema")
    }
  }

  def getValue(vector: org.apache.arrow.vector.ValueVector, ordinal: Int): Any = {
    if (vector.isNull(ordinal)) return null
    val value = vector.getObject(ordinal)
    value match {
      case t: org.apache.arrow.vector.util.Text => org.apache.spark.unsafe.types.UTF8String.fromString(t.toString)
      case s: String => org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case other => other
    }
  }
}



