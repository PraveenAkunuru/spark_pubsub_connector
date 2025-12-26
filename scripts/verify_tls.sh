#!/bin/bash
set -e

# Default explicit CA (system CA for testing validation)
CA_PATH="/etc/ssl/certs/ca-certificates.crt"

echo "=== TLS Verification: Explicit CA Path Check ==="
echo "Using CA Path: $CA_PATH"

# Run the metrics verification script but with the explicit CA path option
# We reuse verify_native_metrics.sh but inject the config?
# verify_native_metrics.sh runs spark-submit.
# We can just copy the command from verify_native_metrics.sh but add the config.

# Find the JAR file dynamically
JAR_PATH=$(find ../spark/spark35/target/scala-2.12 -name "*assembly*.jar" | head -n 1)

if [ -z "$JAR_PATH" ]; then
    echo "Error: Spark assembly JAR not found in $JAR_PATH. Build first."
    exit 1
fi
echo "Using JAR: $JAR_PATH"

PROJECT_ID=$(gcloud config get-value project)
echo "Project ID: $PROJECT_ID"

# 1. Run with explicit valid CA path
echo "1. Testing with VALID explicit CA path..."
${SPARK_HOME}/bin/spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --conf spark.pubsub.caCertificatePath="$CA_PATH" \
  --class com.google.cloud.spark.pubsub.benchmark.PubSubBenchmarkApp \
  "$JAR_PATH" \
  projects/$PROJECT_ID/topics/test-topic-1kb \
  projects/$PROJECT_ID/subscriptions/test-sub-1kb

if [ $? -eq 0 ]; then
    echo "SUCCESS: Explicit CA path verification passed."
else
    echo "FAILURE: Explicit CA path verification failed."
    exit 1
fi

# 2. Run with INVALID explicit CA path (Expect Failure)
echo "2. Testing with INVALID explicit CA path (Expect Failure)..."
set +e
${SPARK_HOME}/bin/spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --conf spark.pubsub.caCertificatePath="/tmp/non_existent_ca.crt" \
  --class com.google.cloud.spark.pubsub.benchmark.PubSubBenchmarkApp \
  "$JAR_PATH" \
  projects/$PROJECT_ID/topics/test-topic-1kb \
  projects/$PROJECT_ID/subscriptions/test-sub-1kb \
  > /tmp/tls_fail_log.txt 2>&1

EXIT_CODE=$?
set -e

if [ $EXIT_CODE -ne 0 ]; then
    echo "SUCCESS: Invalid CA path caused failure as expected."
else
    echo "FAILURE: Invalid CA path did NOT cause failure!"
    exit 1
fi

echo "=== TLS Verification Complete ==="
