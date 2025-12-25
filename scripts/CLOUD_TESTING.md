# Cloud Testing Guide: Spark Pub/Sub Connector (Native)

This guide provides instructions for testing the Spark Pub/Sub connector on actual Google Cloud Platform (GCP) infrastructure using Dataproc and real Pub/Sub topics/subscriptions.

## Prerequisites

1.  **GCP Project**: A GCP project with billing enabled.
2.  **GCS Bucket**: A bucket to store the connector artifacts (JAR and native library).
3.  **Pub/Sub Resources**:
    *   **Topic**: `projects/<PROJECT_ID>/topics/cloud-test-topic`
    *   **Subscription**: `projects/<PROJECT_ID>/subscriptions/cloud-test-sub`
4.  **IAM Permissions**:
    *   `roles/pubsub.subscriber` on the subscription.
    *   `roles/pubsub.publisher` on the topic.
    *   `roles/storage.objectViewer` on the GCS bucket for the Dataproc workers.

## Artifact Preparation

1.  **Build Native Library**:
    ```bash
    cd native
    cargo build --release
    # Result: native/target/release/libpubsub_native.so (Linux)
    ```

2.  **Build Spark Connector**:
    ```bash
    cd spark
    # Assuming sbt is installed
    ./sbt "spark35/assembly"
    # Result: spark/spark35/target/scala-2.12/spark-pubsub-connector-assembly-*.jar
    ```

3.  **Upload to GCS**:
    ```bash
    gsutil cp spark/spark35/target/scala-2.12/*.jar gs://<BUCKET>/connector.jar
    gsutil cp native/target/release/libpubsub_native.so gs://<BUCKET>/libpubsub_native.so
    ```

## Dataproc Cluster Creation

Create a Dataproc cluster (Spark 3.5) with the native library pre-installed or accessible via `--files`.

```bash
gcloud dataproc clusters create cloud-test-cluster \
    --region=<REGION> \
    --image-version=2.2-debian11 \
    --num-workers=2 \
    --worker-machine-type=n2-standard-4 \
    --master-machine-type=n2-standard-4 \
    --initialization-actions=gs://goog-dataproc-initialization-actions-<REGION>/connectors/connectors.sh \
    --metadata=gcs-connector-version=2.2.0
```

## Running Spark Jobs

### Batch Write to Pub/Sub

```bash
gcloud dataproc jobs submit spark \
    --cluster=cloud-test-cluster \
    --region=<REGION> \
    --jars=gs://<BUCKET>/connector.jar \
    --files=gs://<BUCKET>/libpubsub_native.so \
    --properties="spark.executorEnv.LD_LIBRARY_PATH=.,spark.driver.extraLibraryPath=." \
    --class=com.google.cloud.spark.pubsub.examples.BatchWriteExample \
    -- \
    --projectId=<PROJECT_ID> \
    --topicId=cloud-test-topic \
    --count=1000
```

### Streaming Read from Pub/Sub

```bash
gcloud dataproc jobs submit spark \
    --cluster=cloud-test-cluster \
    --region=<REGION> \
    --jars=gs://<BUCKET>/connector.jar \
    --files=gs://<BUCKET>/libpubsub_native.so \
    --properties="spark.executorEnv.LD_LIBRARY_PATH=.,spark.driver.extraLibraryPath=." \
    --class=com.google.cloud.spark.pubsub.examples.StreamingReadExample \
    -- \
    --projectId=<PROJECT_ID> \
    --subscriptionId=cloud-test-sub \
    --checkpointLocation=gs://<BUCKET>/checkpoints/test1
```

## Troubleshooting

- **Native Library Not Found**: Ensure `LD_LIBRARY_PATH` includes the directory where `libpubsub_native.so` is located. Using `--files` with `spark-submit` puts the file in the current working directory of the executor.
- **Authentication**: Dataproc workers use the Service Account associated with the cluster. Ensure it has the necessary Pub/Sub roles.
- **Performance**: Monitor the native logs via `RUST_LOG=info` if needed. Logs from the native layer are forwarded to Spark's standard logging.
