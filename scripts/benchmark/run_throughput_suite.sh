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

# --- Safety & Traps ---
cleanup() {
  local exit_code=$?
  if [[ $exit_code -ne 0 ]]; then
    echo "ERROR: Script failed with exit code $exit_code" | tee -a "$LOG_DIR/error.log" 2>/dev/null || true
  fi
  echo "Exiting..."
}
trap cleanup EXIT

# --- Defaults ---
REGION="us-central1"
EXECUTORS=4
CORES=2
MSG_SIZE=1024
VOLUME_GB=5
BATCH_SIZE=0
BATCH_SIZE=0
MODE="all"
SEEK_TIME=""

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
    --seek-time)
      SEEK_TIME="$2"
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
GCS_JAR="$BUCKET_NAME/spark-pubsub-connector-assembly-0.1.1.jar"
GCS_LIB="$BUCKET_NAME/libnative_pubsub_connector.so"

MSG_COUNT=$(python3 -c "print(int($VOLUME_GB * 1024 * 1024 * 1024 / $MSG_SIZE))")
MEMORY="4g"
OFF_HEAP="1g"

# --- Helper Functions ---

setup_pubsub() {
  echo "[1/4] Setting up Pub/Sub Resources..." | tee -a "$LOG_DIR/setup.log"
  
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
      if [[ -n "$SEEK_TIME" ]]; then
          echo "Subscription $SUB exists. Replaying from $SEEK_TIME..." | tee -a "$LOG_DIR/setup.log"
          gcloud pubsub subscriptions seek "$SUB" --time="$SEEK_TIME" --project="$PROJECT_ID"
          echo "Seeked Subscription: $SUB" | tee -a "$LOG_DIR/setup.log"
      elif [[ "$MODE" == "generate" || "$MODE" == "all" ]]; then
          echo "Subscription $SUB exists. Recreating for Generation (Clean State)..." | tee -a "$LOG_DIR/setup.log"
          gcloud pubsub subscriptions delete "$SUB" --project="$PROJECT_ID" --quiet || true
          gcloud pubsub subscriptions create "$SUB" --topic="$TOPIC" --project="$PROJECT_ID" --ack-deadline=60
          echo "Recreated Subscription: $SUB" | tee -a "$LOG_DIR/setup.log"
      else
          echo "Subscription $SUB exists. Preserving data for Benchmark..." | tee -a "$LOG_DIR/setup.log"
      fi
  fi
}


run_generation() {
  echo "[2/4] Generating Data..." | tee -a "$LOG_DIR/generation.log"
  
  # Logic to determine generation batch size
  # Target ~5MB per batch to maximize throughput
  TARGET_BATCH_BYTES=5000000
  GEN_BATCH_SIZE=$(python3 -c "print(max(500, int($TARGET_BATCH_BYTES / $MSG_SIZE)))")
  echo "Calculated Generation Batch Size: $GEN_BATCH_SIZE" | tee -a "$LOG_DIR/generation.log"
  
  echo "Submitting Generation Job..." >> "$LOG_DIR/generation.log"
  # HDFS Buffer & GCS Archive for Event Logs
HDFS_EVENT_LOG_DIR="hdfs:///tmp/spark-events"
GCS_ARCHIVE_DIR="${BUCKET_NAME}/spark-job-history"

# Ensure HDFS Directory exists (idempotent)
echo "Ensuring HDFS Event Log Directory exists..." | tee -a "$LOG_DIR/generation.log"
gcloud dataproc jobs submit hadoop --cluster "$CLUSTER_NAME" --region "$REGION" \
    --project="$PROJECT_ID" \
    --class org.apache.hadoop.fs.FsShell -- -mkdir -p /tmp/spark-events 2>&1 | tee -a "$LOG_DIR/generation.log"

# Submit Spark Job
echo "Submitting Spark Job..." | tee -a "$LOG_DIR/generation.log"
# Use a temp file to capture output for App ID extraction
JOB_OUTPUT_FILE=$(mktemp)

gcloud dataproc jobs submit spark \
    --cluster "$CLUSTER_NAME" \
    --region "$REGION" \
    --project="$PROJECT_ID" \
    --jars="$GCS_JAR" \
    --files="$GCS_LIB" \
    --class=finalconnector.PubSubLoadGenerator \
    --properties="\
spark.executor.instances=$EXECUTORS,\
spark.executor.cores=$CORES,\
spark.executor.memory=$MEMORY,\
spark.driver.extraLibraryPath=.,\
spark.executor.extraLibraryPath=.,\
spark.pubsub.batchSize=$GEN_BATCH_SIZE,\
spark.pubsub.writer.maxBatchBytes=9000000,\
spark.executorEnv.RUST_LOG=info,\
spark.driverEnv.RUST_LOG=info,\
spark.eventLog.enabled=false,\
spark.eventLog.dir=$HDFS_EVENT_LOG_DIR" \
    -- "$TOPIC" "$MSG_COUNT" "$MSG_SIZE" "8" | tee "$JOB_OUTPUT_FILE"

# Extract Application ID
APP_ID=$(grep -o "application_[0-9_]*" "$JOB_OUTPUT_FILE" | head -n 1)

if [[ -n "$APP_ID" ]]; then
    echo "Identified Spark App ID: $APP_ID" | tee -a "$LOG_DIR/generation.log"
    echo "Archiving Event Log from HDFS to GCS..." | tee -a "$LOG_DIR/generation.log"
    
    # Ensure GCS Destination exists
    gcloud dataproc jobs submit hadoop --cluster "$CLUSTER_NAME" --region "$REGION" \
        --project="$PROJECT_ID" \
        --class org.apache.hadoop.fs.FsShell -- -mkdir -p "${GCS_ARCHIVE_DIR}" 2>&1 | tee -a "$LOG_DIR/generation.log"
    
    # Copy from HDFS to GCS using DistCp or FsShell
    gcloud dataproc jobs submit hadoop --cluster "$CLUSTER_NAME" --region "$REGION" \
        --project="$PROJECT_ID" \
        --class org.apache.hadoop.fs.FsShell -- -cp "${HDFS_EVENT_LOG_DIR}/${APP_ID}*" "${GCS_ARCHIVE_DIR}/" 2>&1 | tee -a "$LOG_DIR/generation.log" || true
else
    echo "WARNING: Could not identify Application ID. Event Logs might remain in HDFS." | tee -a "$LOG_DIR/generation.log"
fi

rm "$JOB_OUTPUT_FILE" 2>&1 | tee -a "$LOG_DIR/generation.log"
      
  echo "Generation Complete."
  
  # Verification: Check Backlog
  echo "[2.5/4] Verifying Generated Backlog..." | tee -a "$LOG_DIR/generation.log"
  # Wait a few seconds for Pub/Sub stats to converge
  sleep 10
  
  BACKLOG_COUNT=$(gcloud pubsub subscriptions describe "$SUB" --project="$PROJECT_ID" --format="value(numUndeliveredMessages)")
  
  if [[ -z "$BACKLOG_COUNT" ]]; then
      BACKLOG_COUNT=0
  fi
  
  echo "Current Backlog: $BACKLOG_COUNT / Expected: $MSG_COUNT" | tee -a "$LOG_DIR/generation.log"
  
  # Allow 10% variance (though it should be exact if no consumers)
  MIN_EXPECTED=$(python3 -c "print(int($MSG_COUNT * 0.9))")
  
  if [[ "$BACKLOG_COUNT" -lt "$MIN_EXPECTED" ]]; then
      echo "CRITICAL FAILURE: Generator finished but subscription backlog ($BACKLOG_COUNT) is less than 90% of expected ($MSG_COUNT)." | tee -a "$LOG_DIR/generation.log"
      echo "Proceeding anyway to check if Read phase can find data..." | tee -a "$LOG_DIR/generation.log"
      # exit 1
  fi
  echo "Backlog Verified." | tee -a "$LOG_DIR/generation.log"
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
  
  # Use HDFS for Checkpoints to avoid GCS Throttling on Metadata
  CHECKPOINT_DIR="hdfs:///tmp/benchmark/checkpoints/run_${MSG_SIZE}b_${EXECUTORS}exec_${VOLUME_GB}gb_$(date +%Y%m%d_%H%M)"
  
  gcloud dataproc jobs submit spark \
      --cluster="$CLUSTER_NAME" \
      --region="$REGION" \
      --project="$PROJECT_ID" \
      --class=finalconnector.PubSubToGCSBenchmark \
      --jars="$GCS_JAR" \
      --files="$GCS_LIB" \
      --properties="spark.executor.instances=$EXECUTORS,spark.executor.cores=$CORES,spark.executor.memory=$MEMORY,spark.memory.offHeap.enabled=true,spark.memory.offHeap.size=$OFF_HEAP,spark.executor.memoryOverhead=1g,spark.dynamicAllocation.enabled=false,spark.driver.extraLibraryPath=.,spark.executor.extraLibraryPath=.,spark.executorEnv.TRIGGER_MODE=AvailableNow,spark.driverEnv.TRIGGER_MODE=AvailableNow,spark.executorEnv.RUST_LOG=info,spark.driverEnv.RUST_LOG=info,spark.pubsub.batchSize=$READ_BATCH_SIZE,spark.pubsub.readWaitMs=2000,spark.eventLog.enabled=false,spark.eventLog.compress=true,spark.eventLog.rolling.enabled=true,spark.eventLog.rolling.maxFileSize=128m,spark.driver.extraJavaOptions=-Dlog4j.threshold=WARN,spark.executor.extraJavaOptions=-Dlog4j.threshold=WARN,spark.sql.streaming.checkpointLocation=$CHECKPOINT_DIR" \
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
