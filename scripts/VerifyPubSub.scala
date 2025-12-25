import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object VerifyPubSub {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Verify Pub/Sub Connector")
      .getOrCreate()

    val subscriptionId = "dataproc-test-sub"

    println(s"Starting Pub/Sub verification for subscription: $subscriptionId")

    val df = spark.readStream
      .format("pubsub-native")
      .option("subscriptionId", subscriptionId)
      .load()

    val query = df.writeStream
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()

    query.awaitTermination(60000) // Run for 1 minute
  }
}
