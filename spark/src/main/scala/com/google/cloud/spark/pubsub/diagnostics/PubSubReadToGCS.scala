package com.google.cloud.spark.pubsub.diagnostics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
 * Simple streaming application to read from Pub/Sub and write to GCS in Parquet format.
 * Usage: PubSubReadToGCS <subscriptionId> <outputDir>
 */
object PubSubReadToGCS {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: PubSubReadToGCS <subscriptionId> <outputDir>")
      System.exit(1)
    }

    val subscriptionId = args(0)
    val outputDir = args(1)

    val spark = SparkSession.builder()
      .appName("PubSubReadToGCS")
      .master("local[*]") // Ensure we run locally for testing
      .getOrCreate()

    println(s"Starting PubSubReadToGCS...")
    println(s"Subscription: $subscriptionId")
    println(s"Output: $outputDir")

    // Read from Pub/Sub
    val df = spark.readStream
      .format("pubsub-native")
      .option("subscriptionId", subscriptionId)
      .option("numPartitions", sys.env.getOrElse("NUM_PARTITIONS", "4"))
      .load()

    // Write to GCS (Parquet)
    val query = df.writeStream
      .format("parquet")
      .option("path", outputDir)
      .option("checkpointLocation", outputDir + "_checkpoint")
      // usage of ProcessingTime("0 seconds") ensures low latency continuous processing
      .trigger(Trigger.ProcessingTime(sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds"))) 
      .start()

    // Monitor loop
    while (query.isActive) {
      val progress = query.lastProgress
      if (progress != null) {
        val nativeMem = new com.google.cloud.spark.pubsub.source.NativeReader().getNativeMemoryUsageNative()
        println(s"Batch: ${progress.batchId} | Rate: ${progress.processedRowsPerSecond} rows/s | Native Mem: ${nativeMem} bytes")
      }
      Thread.sleep(5000)
    }
  }
}
