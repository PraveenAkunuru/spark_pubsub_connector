#!/bin/bash
set -euo pipefail

# ==============================================================================
# Spark Pub/Sub Connector - Throughput Benchmark Suite
# ==============================================================================
# Usage:
#   ./scripts/benchmark/run_throughput_suite.sh [OPTIONS]
#
# Options:
#   --project <ID>          GCP Project ID (Required if PROJECT_ID env not set)
#   --cluster <NAME>        Dataproc Cluster Name (Required if CLUSTER_NAME env not set)
#   --region <REGION>       GCP Region (Default: us-central1)
#   --bucket <BUCKET>       GCS Bucket for Artifacts/Output (Required if BUCKET_NAME env not set)
#   --executors <N>         Number of Spark Executors (Default: 4)
#   --cores <N>             Cores per Executor (Default: 2)
#   --msg-size <BYTES>      Message Size (Default: 1024)
#   --volume-gb <GB>        Total Data Volume in GB (Default: 5)
#   --batch-size <N>        Spark Pub/Sub Batch Size (Default: Adaptive)
#   --mode <MODE>           'generate', 'read', or 'all' (Default: all)
#   --help                  Show this help message
# ==============================================================================

# --- Defaults ---
REGION="us-central1"
EXECUTORS=4
CORES=2
MSG_SIZE=1024
VOLUME_GB=5
BATCH_SIZE=0
MODE="all"

# --- Argument Parsing ---
while [[ $# -gt 0 ]]; do
  case $1 in
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --cluster)
      CLUSTER_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --bucket)
      BUCKET_NAME="$2"
      shift 2
      ;;
    --executors)
      EXECUTORS="$2"
      shift 2
      ;;
    --cores)
      CORES="$2"
      shift 2
      ;;
    --msg-size)
      MSG_SIZE="$2"
      shift 2
      ;;
    --volume-gb)
      VOLUME_GB="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --help)
      echo "Usage: ./scripts/benchmark/run_throughput_suite.sh [OPTIONS]"
      echo "  --project <ID>      GCP Project ID"
      echo "  --cluster <NAME>    Dataproc Cluster Name"
      echo "  --bucket <BUCKET>   GCS Bucket"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# --- Validation ---
if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "Error: PROJECT_ID is not set. Use --project or export PROJECT_ID."
  exit 1
fi
if [[ -z "${CLUSTER_NAME:-}" ]]; then
  echo "Error: CLUSTER_NAME is not set. Use --cluster or export CLUSTER_NAME."
  exit 1
fi
if [[ -z "${BUCKET_NAME:-}" ]]; then
  echo "Error: BUCKET_NAME is not set. Use --bucket or export BUCKET_NAME."
  exit 1
fi

# Ensure Logs Directory Exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
LOG_DIR="$REPO_ROOT/logs/benchmark_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

echo "================================================="
echo " Spark Pub/Sub Throughput Suite"
echo "================================================="
echo " Log Dir:   $LOG_DIR"
echo " Project:   $PROJECT_ID"
echo " Cluster:   $CLUSTER_NAME"
echo " Bucket:    $BUCKET_NAME"
echo " Mode:      $MODE"
echo " Msg Size:  $MSG_SIZE bytes"
echo " Volume:    $VOLUME_GB GB"
echo " Executors: $EXECUTORS ($CORES cores each)"
echo "================================================="

# --- Derived Config ---
TOPIC="benchmark-throughput-${MSG_SIZE}b"
SUB="benchmark-sub-${MSG_SIZE}b"
GCS_JAR="$BUCKET_NAME/spark-pubsub-connector-assembly-test.jar"
GCS_LIB="$BUCKET_NAME/libnative_pubsub_connector.so"

MSG_COUNT=$(python3 -c "print(int($VOLUME_GB * 1024 * 1024 * 1024 / $MSG_SIZE))")
MEMORY="4g"
OFF_HEAP="1g"

# --- Helper Functions ---

setup_pubsub() {
  echo "[1/4] Setting up Pub/Sub Resources..." | tea -a "$LOG_DIR/setup.log"
  
  if ! gcloud pubsub topics describe "$TOPIC" --project="$PROJECT_ID" >/dev/null 2>&1; then
      gcloud pubsub topics create "$TOPIC" --project="$PROJECT_ID"
      echo "Created Topic: $TOPIC" | tee -a "$LOG_DIR/setup.log"
  else
      echo "Topic $TOPIC exists." | tee -a "$LOG_DIR/setup.log"
  fi

  if ! gcloud pubsub subscriptions describe "$SUB" --project="$PROJECT_ID" >/dev/null 2>&1; then
      gcloud pubsub subscriptions create "$SUB" --topic="$TOPIC" --project="$PROJECT_ID" --ack-deadline=60
      echo "Created Subscription: $SUB" | tee -a "$LOG_DIR/setup.log"
  else
      echo "Subscription $SUB exists. Purging..." | tee -a "$LOG_DIR/setup.log"
      gcloud pubsub subscriptions seek "$SUB" --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --project="$PROJECT_ID"
      echo "Purged Subscription: $SUB" | tee -a "$LOG_DIR/setup.log"
  fi
}

run_generation() {
  echo "[2/4] Generating Data..." | tee -a "$LOG_DIR/generation.log"
  
  # Logic to determine generation batch size (write side needs smaller batches usually)
  GEN_BATCH_SIZE=500  # Default safe write batch size
  
  echo "Submitting Generation Job..." >> "$LOG_DIR/generation.log"
  gcloud dataproc jobs submit spark \
      --cluster="$CLUSTER_NAME" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --class=finalconnector.PubSubLoadGenerator \
      --jars="$GCS_JAR" \
      --files="$GCS_LIB" \
      --properties="spark.executor.instances=4,spark.executor.cores=$CORES,spark.executor.memory=$MEMORY,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.pubsub.writer.maxBatchBytes=9000000,spark.pubsub.batchSize=$GEN_BATCH_SIZE" \
      -- "$TOPIC" "$MSG_COUNT" "$MSG_SIZE" "8" 2>&1 | tee -a "$LOG_DIR/generation.log"
      
  echo "Generation Complete."
}

run_benchmark() {
  echo "[3/4] Running Read Benchmark..." | tee -a "$LOG_DIR/benchmark.log"
  
  # Determine Read Batch Size
  if [[ "$BATCH_SIZE" -gt 0 ]]; then
      READ_BATCH_SIZE=$BATCH_SIZE
  elif [[ "$MSG_SIZE" -ge 9000 ]]; then
      READ_BATCH_SIZE=5000 # Optimized for 10KB
  elif [[ "$MSG_SIZE" -ge 4000 ]]; then
      READ_BATCH_SIZE=5000 # Optimized for 4KB
  else
      READ_BATCH_SIZE=50000 # Optimized for 1KB
  fi
  echo "Using Read Batch Size: $READ_BATCH_SIZE" | tee -a "$LOG_DIR/benchmark.log"

  OUT_DIR="$BUCKET_NAME/output/run_${MSG_SIZE}b_${EXECUTORS}exec_${VOLUME_GB}gb_$(date +%Y%m%d_%H%M)"
  
  gcloud dataproc jobs submit spark \
      --cluster="$CLUSTER_NAME" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --class=finalconnector.PubSubToGCSBenchmark \
      --jars="$GCS_JAR" \
      --files="$GCS_LIB" \
      --properties="spark.executor.instances=$EXECUTORS,spark.executor.cores=$CORES,spark.executor.memory=$MEMORY,spark.memory.offHeap.enabled=true,spark.memory.offHeap.size=$OFF_HEAP,spark.executor.memoryOverhead=1g,spark.dynamicAllocation.enabled=false,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.executorEnv.TRIGGER_MODE=AvailableNow,spark.pubsub.batchSize=$READ_BATCH_SIZE,spark.pubsub.readWaitMs=2000" \
      -- "$SUB" "$OUT_DIR" "$MSG_SIZE" 2>&1 | tee -a "$LOG_DIR/benchmark.log"

  echo "[4/4] Benchmark Job Finished."
  echo "Output Directory: $OUT_DIR"
}

# --- Main Execution ---

setup_pubsub

if [[ "$MODE" == "generate" || "$MODE" == "all" ]]; then
  run_generation
fi

if [[ "$MODE" == "read" || "$MODE" == "all" ]]; then
  run_benchmark
fi

echo "================================================="
echo " Suite Completed."
echo " Logs available in: $LOG_DIR"
echo "================================================="
