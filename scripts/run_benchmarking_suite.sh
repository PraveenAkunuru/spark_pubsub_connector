#!/bin/bash
set -e

PROJECT_ID="pakunuru-1119-20250930202256"
CLUSTER="cluster-be84"
REGION="us-central1"
BUCKET="gs://pakunuru-spark-pubsub-benchmark"
JAR="$BUCKET/spark-pubsub-connector-3.5-assembly-0.1.0.jar"
LIB="$BUCKET/libnative_pubsub_v3.so"

# TARGET: ~10GB of data for each size
# 1KB  -> 10 * 1024 * 1024 / 1 = 10,485,760 messages
# 4KB  -> 10 * 1024 * 1024 / 4 = 2,621,440 messages
# 10KB -> 10 * 1024 * 1024 / 10 = 1,048,576 messages

SIZES=(1024 4096 10240)
LABELS=("1kb" "4kb" "10kb")
# Using slightly rounded values for clarity
COUNTS=(10000000 2500000 1000000)

echo "Starting High-Volume Benchmarking Suite (10GB per size)..."

for i in "${!SIZES[@]}"; do
    SIZE=${SIZES[$i]}
    LABEL=${LABELS[$i]}
    COUNT=${COUNTS[$i]}
    TOPIC="benchmark-throughput-$LABEL"
    SUB="benchmark-sub-$LABEL"
    OUT="$BUCKET/output/final_run_$LABEL"
    
    # Calculate safe read batch size (Target ~8MB to stay 20% below 10MB limit)
    READ_BATCH=1000
    if [ "$LABEL" == "1kb" ]; then READ_BATCH=8000; fi
    if [ "$LABEL" == "4kb" ]; then READ_BATCH=2000; fi
    if [ "$LABEL" == "10kb" ]; then READ_BATCH=800; fi

    echo "================================================="
    echo "BENCHMARKING SIZE: $LABEL ($SIZE bytes)"
    echo "Config: WriteCount=$COUNT, ReadBatch=$READ_BATCH"
    echo "================================================="
    
    # 1. Purge Subscription (to start fresh)
    echo "Purging subscription $SUB..."
    gcloud pubsub subscriptions seek "$SUB" --time=$(date -u +%Y-%m-%dT%H:%M:%SZ) --project=$PROJECT_ID || true
    
    # 2. Run Write Benchmark
    echo "Running Write Benchmark for $LABEL..."
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --region=$REGION \
        --project=$PROJECT_ID \
        --class=com.google.cloud.spark.pubsub.benchmark.PubSubLoadGenerator \
        --jars=$JAR \
        --files=$LIB \
        --properties="spark.executor.cores=4,spark.executor.memory=8g,spark.yarn.executor.memoryOverhead=4g,spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executorEnv.LD_LIBRARY_PATH=.,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=." \
        -- "$TOPIC" "$COUNT" "$SIZE"
    
    # 3. Run Read Benchmark (Optimized Batching)
    echo "Running Read Benchmark for $LABEL..."
    # We use AvailableNow trigger to finish as soon as data is processed
    gcloud dataproc jobs submit spark \
        --cluster=$CLUSTER \
        --region=$REGION \
        --project=$PROJECT_ID \
        --class=com.google.cloud.spark.pubsub.benchmark.PubSubToGCSBenchmark \
        --jars=$JAR \
        --files=$LIB \
        --properties="spark.executor.cores=4,spark.executor.memory=8g,spark.yarn.executor.memoryOverhead=4g,spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executorEnv.LD_LIBRARY_PATH=.,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.executorEnv.TRIGGER_MODE=AvailableNow,spark.pubsub.batchSize=$READ_BATCH,spark.pubsub.readWaitMs=2000" \
        -- "$SUB" "$OUT" "$SIZE"

    echo "Finished benchmarking $LABEL"
done

echo "High-Volume Benchmarking Suite Complete."
