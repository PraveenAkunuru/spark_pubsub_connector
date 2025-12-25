package com.google.cloud.spark.pubsub

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class ThroughputIntegrationTest extends AnyFunSuite {
  // These are now defined inside the test method to allow for sys.env overrides
  // val projectId = sys.props.getOrElse("pubsub.project.id", "throughput-test-project")
  // val subscriptionId = sys.props.getOrElse("pubsub.subscription.id", "throughput-sub")
  // val targetCount = sys.props.getOrElse("pubsub.msg.count", "50000").toInt

  test("Throughput measurement - Configurable") {
    val spark = SparkSession.builder()
      .appName("ThroughputIntegrationTest")
      .master(sys.env.getOrElse("TEST_MASTER", sys.props.getOrElse("spark.master", "local[4]")))
      .config("spark.sql.vectorized.enabled", "true")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val projectId = sys.env.getOrElse("PUBSUB_PROJECT_ID", sys.props.getOrElse("pubsub.project.id", "throughput-test-project"))
    val subscriptionId = sys.env.getOrElse("PUBSUB_SUBSCRIPTION_ID", sys.props.getOrElse("pubsub.subscription.id", "throughput-sub"))
    val targetCount = sys.env.getOrElse("PUBSUB_MSG_COUNT", sys.props.getOrElse("pubsub.msg.count", "50000")).toInt
    val payloadSize = sys.env.getOrElse("PUBSUB_PAYLOAD_SIZE", sys.props.getOrElse("pubsub.payload.size", "1024")).toInt

    try {
      val df = spark.readStream
        .format("pubsub-native")
        .option("projectId", projectId)
        .option("subscriptionId", subscriptionId)
        .option("numPartitions", "10")
        .load()

      val query = df.writeStream
          .format("memory")
          .queryName("throughput_test_table")
          .start()

      // Target count
      val target = targetCount
      var count = 0L
      val startTime = System.currentTimeMillis()
      val timeout = 120000 // 120 seconds
      
      while (count < target && (System.currentTimeMillis() - startTime) < timeout) {
        Thread.sleep(1000)
        count = spark.sql("SELECT * FROM throughput_test_table").count()
        println(s"Current Count: $count")
      }
      
      val endTime = System.currentTimeMillis()
      val durationMs = endTime - startTime
      val durationSec = durationMs / 1000.0
      
      val msgPerSec = count / durationSec
      // We don't know payload size in Spark exactly without guessing, but we can just report Msg/sec
      // MB/s calculation in previous test assumed 1KB. We can make it generic or just remove it.
      // Or we can pass payload size too.
      val payloadSize = sys.props.getOrElse("pubsub.payload.size", "1024").toInt
      val mbPerSec = (count * payloadSize.toDouble) / (1024.0 * 1024.0) / durationSec
      
      println("======================================================")
      println(s"Throughput Results (Local Machine):")
      println(s"Total Messages: $count")
      println(s"Total Time: $durationSec seconds")
      println(f"Messages/sec: $msgPerSec%.2f")
      println(f"Throughput: $mbPerSec%.2f MB/s")
      println("======================================================")
      
      query.stop()
      
      assert(count >= target, s"Throughput test failed: Expected $target but got $count")
      
    } finally {
      spark.stop()
    }
  }
}
