package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object CloudSmoketest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: CloudSmoketest <subscriptionId>")
      sys.exit(1)
    }
    val subscriptionId = args(0)
    
    val spark = SparkSession.builder()
      .appName(s"Cloud Smoketest - $subscriptionId")
      .getOrCreate()

    println(s"Starting Cloud Smoketest for subscription: $subscriptionId")

    val df = spark.readStream
      .format("pubsub-native")
      .option("subscriptionId", subscriptionId)
      .load()

    val query = df.writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    println("Streaming query started. Waiting for 60 seconds...")
    query.awaitTermination(60000)
    println("Smoketest completed.")
    query.stop()
  }
}
