package finalconnector

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{BinaryType, StringType, StructType, TimestampType, MapType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.sources.DataSourceRegister
import scala.collection.JavaConverters._

import java.util

/**
 * Spark DataSource V2 TableProvider for Google Cloud Pub/Sub.
 *
 * This is the main entry point for the connector. It registers the "pubsub-native-v2"
 * short name and manages the lifecycle of `PubSubTable` instances.
 *
 * ## Architecture
 * The provider implements a "Fail-Fast" design where critical requirements
 * (liking Application Default Credentials) are verified before the Spark job starts.
 */
class PubSubTableProviderV2 extends TableProvider with DataSourceRegister with org.apache.spark.internal.Logging {
  
  override def shortName(): String = "pubsub-native-v2"

  /**
   * Defines the default schema for Pub/Sub messages.
   *
   * @param options Data source options
   * @return The standard Pub/Sub metadata schema
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    logInfo("Pub/Sub: Inferring standard metadata schema (Raw Mode)")
    new StructType()
      .add("message_id", StringType, nullable = true)
      .add("publish_time", TimestampType, nullable = true)
      .add("payload", BinaryType, nullable = true)
      .add("ack_id", StringType, nullable = true)
      .add("attributes", MapType(StringType, StringType), nullable = true)
  }

  /**
   * Instantiates a `PubSubTable` for reading or writing.
   *
   * @param passedSchema The schema requested by the user or inferred
   * @param partitioning Partitioning transforms (ignored as Pub/Sub is inherently non-partitioned on write)
   * @param properties   Configuration properties from `.option()` or `.config()`
   * @return A configured PubSubTable instance
   */
  override def getTable(passedSchema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    // If no specific schema is passed (e.g. .load()), we use the standard metadata schema.
    val schema = if (passedSchema != null && passedSchema.nonEmpty) passedSchema else inferSchema(new CaseInsensitiveStringMap(new java.util.HashMap()))
    
    // Fail-Fast: Verify Credentials locally on the driver to avoid distributed failures later.
    try {
      val creds = com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
      logInfo("Pub/Sub: Fail-Fast Auth check passed.")
    } catch {
      case e: Throwable =>
        throw new RuntimeException("Pub/Sub Auth Failure: Failed to load Google Application Default Credentials. " +
          "Ensure GOOGLE_APPLICATION_CREDENTIALS is set or 'gcloud auth application-default login' has been run.", e)
    }

    val spark = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(null)
    val projId = PubSubConfig.getOption(PubSubConfig.PROJECT_ID_KEY, properties.asScala.toMap, spark).getOrElse("autodetected")
    val subId = PubSubConfig.getOption(PubSubConfig.SUBSCRIPTION_ID_KEY, properties.asScala.toMap, spark).orNull
    val topicId = PubSubConfig.getOption(PubSubConfig.TOPIC_ID_KEY, properties.asScala.toMap, spark).orNull

    // Validate that at least one target is specified.
    if ((subId == null || subId.trim.isEmpty) && (topicId == null || topicId.trim.isEmpty)) {
      throw new IllegalArgumentException("Metadata Error: Must provide either 'subscriptionId' (for read) or 'topicId' (for write).")
    }
    
    val target = if (subId != null) s"subscription $subId" else s"topic $topicId"
    logInfo(s"Pub/Sub: Initializing table for $target (Project: $projId)")
    
    new PubSubTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}
