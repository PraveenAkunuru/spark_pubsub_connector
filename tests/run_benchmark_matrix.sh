#!/bin/bash
set -e

# Usage: ./tests/run_benchmark_matrix.sh <EXECUTORS> <MSG_SIZE_BYTES> <VOLUME_GB> <CORES_PER_EXEC>
# Example: ./tests/run_benchmark_matrix.sh 4 1024 5 2

EXECUTORS=${1:-4}
MSG_SIZE=${2:-1024}
VOLUME_GB=${3:-5}
CORES=${4:-2}

# Configuration
PROJECT_ID="pakunuru-1119-20250930202256"
CLUSTER="cluster-be84"
REGION="us-central1"
BUCKET="gs://pakunuru-spark-pubsub-benchmark"
GCS_JAR="$BUCKET/spark-pubsub-connector-assembly-test.jar"
GCS_LIB="$BUCKET/libnative_pubsub_connector.so"

TOPIC="benchmark-throughput-${MSG_SIZE}b"
SUB="benchmark-sub-${MSG_SIZE}b"

# Calculate Message Count
# GB * 1024^3 / MSG_SIZE
MSG_COUNT=$(python3 -c "print(int($VOLUME_GB * 1024 * 1024 * 1024 / $MSG_SIZE))")

# CORES is now set via argument above
MEMORY="4g"
OFF_HEAP="1g"

echo "================================================="
echo " STARTING BENCHMARK MATRIX RUN"
echo "================================================="
echo "Config:"
echo " Cluster:   $CLUSTER"
echo " Topic:     $TOPIC"
echo " Sub:       $SUB"
echo " MsgSize:   $MSG_SIZE bytes"
echo " Volume:    $VOLUME_GB GB"
echo " MsgCount:  $MSG_COUNT"
echo " Executors: $EXECUTORS"
echo " Cores/Exec:$CORES"
echo "================================================="

# 1. Setup Pub/Sub
echo "[1/4] Setting up Pub/Sub..."
if ! gcloud pubsub topics describe "$TOPIC" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud pubsub topics create "$TOPIC" --project="$PROJECT_ID"
else
    echo "Topic $TOPIC exists."
fi

if ! gcloud pubsub subscriptions describe "$SUB" --project="$PROJECT_ID" >/dev/null 2>&1; then
    gcloud pubsub subscriptions create "$SUB" --topic="$TOPIC" --project="$PROJECT_ID" --ack-deadline=60
else
    echo "Subscription $SUB exists. Purging..."
    gcloud pubsub subscriptions seek "$SUB" --time=$(date -u +%Y-%m-%dT%H:%M:%SZ) --project="$PROJECT_ID"
fi

BATCH_SIZE_ARG=${5:-0}

# 2. Generate Data
# Adaptive Batch Size for Large Messages
if [ "$BATCH_SIZE_ARG" -gt 0 ]; then
    PUB_SUB_BATCH_SIZE=$BATCH_SIZE_ARG
    echo "Using explicit batch size: $PUB_SUB_BATCH_SIZE."
elif [ "$MSG_SIZE" -ge 9000 ]; then
    PUB_SUB_BATCH_SIZE=500
    echo "Large message size detected ($MSG_SIZE bytes). Limiting batch size to $PUB_SUB_BATCH_SIZE."
else
    PUB_SUB_BATCH_SIZE=2000
    echo "Using default batch size: $PUB_SUB_BATCH_SIZE."
fi

echo "[2/4] Generating Data ($VOLUME_GB GB)..."
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --region=$REGION \
    --project=$PROJECT_ID \
    --class=finalconnector.PubSubLoadGenerator \
    --jars=$GCS_JAR \
    --files=$GCS_LIB \
    --properties="spark.executor.instances=4,spark.executor.cores=$CORES,spark.executor.memory=$MEMORY,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.pubsub.writer.maxBatchBytes=9000000,spark.pubsub.batchSize=$PUB_SUB_BATCH_SIZE" \
    -- "$TOPIC" "$MSG_COUNT" "$MSG_SIZE" "8"

echo "Data Generation Complete."

# 3. Read Benchmark
echo "[3/4] Starting Read Benchmark..."
OUT_DIR="$BUCKET/output/run_${MSG_SIZE}b_${EXECUTORS}exec_${VOLUME_GB}gb_$(date +%Y%m%d_%H%M)"

gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER \
    --region=$REGION \
    --project=$PROJECT_ID \
    --class=finalconnector.PubSubToGCSBenchmark \
    --jars=$GCS_JAR \
    --files=$GCS_LIB \
    --properties="spark.executor.instances=$EXECUTORS,spark.executor.cores=$CORES,spark.executor.memory=$MEMORY,spark.memory.offHeap.enabled=true,spark.memory.offHeap.size=$OFF_HEAP,spark.executor.memoryOverhead=1g,spark.dynamicAllocation.enabled=false,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.executorEnv.TRIGGER_MODE=AvailableNow,spark.pubsub.batchSize=$PUB_SUB_BATCH_SIZE,spark.pubsub.readWaitMs=2000" \
    -- "$SUB" "$OUT_DIR" "$MSG_SIZE"

echo "[4/4] Benchmark Job Submitted."
echo "Check output in: $OUT_DIR"
