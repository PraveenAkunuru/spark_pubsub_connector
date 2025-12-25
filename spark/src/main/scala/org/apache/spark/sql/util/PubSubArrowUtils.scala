package org.apache.spark.sql.util

import org.apache.spark.sql.types.StructType

object PubSubArrowUtils {
  def toArrowSchema(schema: StructType): String = {
    try {
      // Try Spark 3.4+ signature (4 args)
      val method = ArrowUtils.getClass.getMethod("toArrowSchema", classOf[StructType], classOf[String], classOf[Boolean], classOf[Boolean])
      method.invoke(ArrowUtils, schema, "UTC", Boolean.box(true), Boolean.box(false)).asInstanceOf[org.apache.arrow.vector.types.pojo.Schema].toJson()
    } catch {
      case _: NoSuchMethodException =>
        try {
          // Try Spark 3.3 signature (2 args)
          val method = ArrowUtils.getClass.getMethod("toArrowSchema", classOf[StructType], classOf[String])
          method.invoke(ArrowUtils, schema, "UTC").asInstanceOf[org.apache.arrow.vector.types.pojo.Schema].toJson()
        } catch {
          case e: Exception =>
            throw new RuntimeException(s"Failed to convert schema to Arrow JSON via ArrowUtils: ${e.getMessage}", e)
        }
      case e: Exception => throw new RuntimeException(s"Error invoking ArrowUtils: ${e.getMessage}", e)
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



