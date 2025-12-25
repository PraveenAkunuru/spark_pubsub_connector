package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.spark.sql.types._

object ExecutorDiag {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ExecutorDiag").getOrCreate()
    val sc = spark.sparkContext

    println("--- DRIVER DIAG START ---")
    sys.env.filter(_._1.startsWith("GOOGLE")).foreach(println)
    sys.env.filter(_._1.startsWith("PUBSUB")).foreach(println)
    runDiag(0) // Run on driver
    println("--- DRIVER DIAG END ---")

    val results = sc.parallelize(1 to 1, 1).map { i =>
      runDiag(i)
    }.collect()

    results.foreach(println)
    spark.stop()
  }

  def runDiag(id: Int): String = {
      val reader = new NativeReader()
      val projectId = "pakunuru-1119-20250930202256"
      val subId = "dataproc-test-sub"
      
      val schema = StructType(Seq(
        StructField("data", BinaryType, true),
        StructField("message_id", StringType, true),
        StructField("publish_time", TimestampType, true),
        StructField("attributes", StringType, true)
      ))
      val schemaJson = PubSubConfig.buildProcessingConfigJson(schema, Some("json"), None)
      
      val loadRes = try {
        NativeLoader.load()
        s"NativeLoader.load success (Schema: $schemaJson)"
      } catch {
        case e: Throwable => s"NativeLoader.load fail: ${e.getMessage}"
      }

      val initResult = try {
        val ptr = reader.init(projectId, subId, 0, schemaJson)
        if (ptr != 0 && ptr != -1) {
          val allocator = new RootAllocator()
          val arrowArray = ArrowArray.allocateNew(allocator)
          val arrowSchema = ArrowSchema.allocateNew(allocator)
          try {
            Thread.sleep(5000)
            val batchRes = reader.getNextBatch(ptr, "batch-diag", arrowArray.memoryAddress(), arrowSchema.memoryAddress())
            
            if (batchRes > 0) {
               val root = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, null)
               val count = root.getRowCount
               root.close()
               
               // Verify Acknowledgment
               val ackRes = reader.ackCommitted(ptr, java.util.Collections.singletonList("batch-diag"))
               s"Success: ptr=$ptr, getNextBatch=$batchRes, rowCount=$count, ackRes=$ackRes"
            } else {
               s"Success: ptr=$ptr, getNextBatch=$batchRes (empty)"
            }
          } catch {
            case e: Throwable => 
               val sw = new java.io.StringWriter()
               e.printStackTrace(new java.io.PrintWriter(sw))
               s"Success: ptr=$ptr, getNextBatch fail: ${e.getMessage}\n${sw.toString}"
          } finally {
            reader.close(ptr)
            arrowArray.close()
            arrowSchema.close()
            allocator.close()
          }
        } else {
          s"init returned $ptr (0=Auth/Validate Fail, -1=Check logs)"
        }
      } catch {
        case e: Throwable => 
          val sw = new java.io.StringWriter()
          e.printStackTrace(new java.io.PrintWriter(sw))
          s"init fail: ${e.getMessage}\n${sw.toString}"
      }
      
      val label = if (id == 0) "Driver" else s"Executor $id"
      val diskWriteRes = try {
        val pw = new java.io.PrintWriter(new java.io.File("/tmp/executor_diag_disk_check.txt"))
        pw.write(s"Diagnostic run at ${new java.util.Date()}\n")
        pw.write(s"Label: $label\n")
        pw.write(s"Init Result: $initResult\n")
        pw.close()
        "Disk Write Success"
      } catch {
        case e: Throwable => s"Disk Write Fail: ${e.getMessage}"
      }
      
      val res = s"$label: Load=$loadRes, Result=$initResult, Disk=$diskWriteRes"
      println(res)
      res
  }
}
