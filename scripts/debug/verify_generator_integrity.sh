#!/bin/bash
set -euo pipefail

# Config
PROJECT_ID=${PROJECT_ID:-"pakunuru-1119-20250930202256"}
CLUSTER_NAME=${CLUSTER_NAME:-"cluster-3ec7"}
BUCKET_NAME=${BUCKET_NAME:-"gs://pakunuru-spark-pubsub-benchmark"}
REGION="us-central1"
TOPIC="debug-gen-integrity"
SUB="debug-sub-integrity"
MSG_SIZE=2048
MSG_COUNT=100000 # 200 MB

echo "================================================="
echo " Generator Integrity Test"
echo " Msg Count: $MSG_COUNT, Size: $MSG_SIZE"
echo "================================================="

# 1. Setup Topic/Sub
gcloud pubsub subscriptions delete "$SUB" --project="$PROJECT_ID" --quiet || true
gcloud pubsub topics delete "$TOPIC" --project="$PROJECT_ID" --quiet || true
# gcloud pubsub topics create "$TOPIC" --project="$PROJECT_ID"
gcloud pubsub subscriptions create "$SUB" --topic="$TOPIC" --project="$PROJECT_ID" --ack-deadline=60
echo "Topic and Subscription Recreated (Clean Slate)."


# 2. Run Generator
echo "[2/4] Running Generator Job..."
GCS_JAR="$BUCKET_NAME/spark-pubsub-connector-assembly-0.1.1.jar"
GCS_LIB="$BUCKET_NAME/libnative_pubsub_connector.so"

gcloud dataproc jobs submit spark \
    --cluster "$CLUSTER_NAME" \
    --region "$REGION" \
    --project="$PROJECT_ID" \
    --jars="$GCS_JAR" \
    --files="$GCS_LIB" \
    --class=finalconnector.PubSubLoadGenerator \
    --properties="spark.executor.instances=2,spark.executor.cores=2,spark.executor.memory=4g,spark.pubsub.batchSize=1000,spark.pubsub.writer.maxBatchBytes=5000000,spark.driverEnv.RUST_LOG=info,spark.executorEnv.RUST_LOG=info" \
    -- "$TOPIC" "$MSG_COUNT" "$MSG_SIZE" "4"


# 3. Verify Backlog
echo "[3/4] Verifying Backlog..."
# We use pull --limit=10 to see if we get ANY data.
# Ideally we want to count. We can't easily count without pulling all.
# We will pull 10 messages. If we get 10, it's a good sign.
# If we get 0, IT FAILED.

echo "Pulling sample messages..."
PULL_OUTPUT=$(gcloud pubsub subscriptions pull "$SUB" --project="$PROJECT_ID" --limit=10 --auto-ack --format="value(messageId)" 2>/dev/null)
COUNT=$(echo "$PULL_OUTPUT" | wc -w)

echo "Pulled $COUNT messages."

if [[ "$COUNT" -gt 0 ]]; then
    echo "SUCCESS: Generator produced data."
else
    echo "FAILURE: Generator finished but Subscription is empty."
    exit 1
fi
