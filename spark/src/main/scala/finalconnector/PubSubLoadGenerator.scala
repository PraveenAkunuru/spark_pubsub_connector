package finalconnector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object PubSubLoadGenerator {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: PubSubLoadGenerator <topicId> <numMessages> <msgSizeBytes> [numPartitions]")
      System.exit(1)
    }

    val topicId = args(0)
    val numMessages = args(1).toLong
    val msgSizeBytes = args(2).toInt
    // Default to 8 partitions if not specified (safe for 4 executors)
    val numPartitions = if (args.length >= 4) args(3).toInt else 8

    val spark = SparkSession.builder()
      .appName("PubSubLoadGenerator")
      .getOrCreate()

    import spark.implicits._

    println(s"Generating $numMessages messages of size $msgSizeBytes bytes to $topicId using $numPartitions partitions...")

    // Use range with explicit partitions to avoid shuffle
    val df = spark.range(0, numMessages, 1, numPartitions)
      .mapPartitions { iter =>
         // Log start of partition processing
         val taskId = org.apache.spark.TaskContext.getPartitionId()
         println(s"Executor: Starting partition $taskId")
         
         val bytes = "a" * msgSizeBytes
         val binary = bytes.getBytes("UTF-8")
         
         // Use a mapped iterator to be lazy/streaming
         iter.map { id => 
             if (id % 100000 == 0) {
                 println(s"Partition $taskId processed $id messages")
             }
             (binary, System.currentTimeMillis()) 
         }
      }
      .toDF("payload", "publish_time_ts") // Schema: payload: Binary
      .withColumn("publish_time", ($"publish_time_ts" / 1000).cast(TimestampType))
      .drop("publish_time_ts")

    // Write to Pub/Sub
    df.write
      .format("pubsub-native-v2")
      .mode("append")
      .option("topicId", topicId)
      .save()

    println("Generation Complete.")
    spark.stop()
  }
}
