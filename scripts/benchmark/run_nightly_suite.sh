#!/bin/bash
set -euo pipefail

# ==============================================================================
# Nightly Sustained Throughput Suite
# Runs 2KB -> 4KB -> 6KB -> 8KB sequentially.
# ==============================================================================

PROJECT_ID=${PROJECT_ID:-"pakunuru-1119-20250930202256"}
CLUSTER_NAME=${CLUSTER_NAME:-"cluster-3ec7"}
BUCKET_NAME=${BUCKET_NAME:-"gs://pakunuru-spark-pubsub-benchmark"}
REGION=${REGION:-"us-central1"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")/logs/nightly_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$LOG_DIR"

echo "=================================================" | tee -a "$LOG_DIR/suite.log"
echo " Starting Nightly Sustained Suite (V6 - Hardened)" | tee -a "$LOG_DIR/suite.log"
echo " Cluster: $CLUSTER_NAME" | tee -a "$LOG_DIR/suite.log"
echo " Logs:    $LOG_DIR" | tee -a "$LOG_DIR/suite.log"
echo "=================================================" | tee -a "$LOG_DIR/suite.log"

ensure_cluster_idle() {
    echo "Checking cluster state..." | tee -a "$LOG_DIR/suite.log"
    ATTEMPTS=0
    while true; do
        ACTIVE_JOBS=$(gcloud dataproc jobs list --cluster="$CLUSTER_NAME" --project="$PROJECT_ID" --region="$REGION" --state-filter=active --format="value(reference.jobId)")
        
        if [[ -z "$ACTIVE_JOBS" ]]; then
            echo "Cluster is IDLE. Proceeding." | tee -a "$LOG_DIR/suite.log"
            break
        else
            echo "WARNING: Found Active Jobs: $ACTIVE_JOBS" | tee -a "$LOG_DIR/suite.log"
            echo "Forcing Cleanup..." | tee -a "$LOG_DIR/suite.log"
            echo "$ACTIVE_JOBS" | xargs -I {} gcloud dataproc jobs kill {} --project="$PROJECT_ID" --region="$REGION" --quiet || true
            
            # Simple timeout mechanism
            ATTEMPTS=$((ATTEMPTS+1))
            if [[ $ATTEMPTS -gt 10 ]]; then
                echo "ERROR: Unable to clear cluster after 10 attempts. Manual intervention required." | tee -a "$LOG_DIR/suite.log"
                exit 1
            fi
            
            echo "Waiting 10s for termination..."
            sleep 10
        fi
    done
}


run_msg_size() {
    SIZE=$1
    VOL=$2
    THRESHOLD=$3
    LOG_FILE="$LOG_DIR/run_${SIZE}b.log"
    
    # Pre-Run Cleanup
    ensure_cluster_idle
    
    echo "-------------------------------------------------" | tee -a "$LOG_DIR/suite.log"
    echo " Starting $SIZE bytes Config (Vol: $VOL GB)..." | tee -a "$LOG_DIR/suite.log"
    echo "-------------------------------------------------" | tee -a "$LOG_DIR/suite.log"
    
    START_TIME=$(date +%s)
    
    # Run Benchmark
    "$SCRIPT_DIR/run_throughput_suite.sh" --msg-size "$SIZE" --volume-gb "$VOL" --mode all > "$LOG_FILE" 2>&1
    
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    # Verify Result
    if python3 "$SCRIPT_DIR/analyze_msg_throughput.py" "$LOG_FILE" "$THRESHOLD"; then
        STATUS="PASS"
        echo " [PASS] $SIZE bytes Verification Succeeded." | tee -a "$LOG_DIR/suite.log"
    else
        STATUS="FAIL"
        echo " [FAIL] $SIZE bytes Verification Failed (See $LOG_FILE)." | tee -a "$LOG_DIR/suite.log"
    fi
    
    # Parse Result for Report (grep from the python output isn't easy here unless we capture it, 
    # but the python script prints to stdout. Let's capture it.)
    # Rerunning extraction for report simplicity:
    MB_S=$(python3 "$SCRIPT_DIR/analyze_msg_throughput.py" "$LOG_FILE" "0" | grep "RESULT_MB_S=" | cut -d= -f2)
    
    # Append to Report
    echo "| **$((SIZE / 1024)) KB** | $VOL GB | ${DURATION}s | **$MB_S MB/s** | **$STATUS** | Threshold: $THRESHOLD MB/s |" >> "$LOG_DIR/nightly_report.md"
    
    # Sleep to cool down / cleanup
    echo " Cooling down for 60s..." | tee -a "$LOG_DIR/suite.log"
    sleep 60
}

export PROJECT_ID CLUSTER_NAME BUCKET_NAME REGION

# Initialize Report
echo "# Nightly Sustained Throughput Report" > "$LOG_DIR/nightly_report.md"
echo "**Date:** $(date)" >> "$LOG_DIR/nightly_report.md"
echo "**Cluster:** $CLUSTER_NAME" >> "$LOG_DIR/nightly_report.md"
echo "" >> "$LOG_DIR/nightly_report.md"
echo "| Message Size | Volume | Duration | Throughput | Status | Notes |" >> "$LOG_DIR/nightly_report.md"
echo "| :--- | :--- | :--- | :--- | :--- | :--- |" >> "$LOG_DIR/nightly_report.md"

# 1. 2KB Sustained (8 GB, Expect > 3 MB/s - conservative due to single thread or overhead)
run_msg_size 2048 8 3.0

# 2. 4KB Sustained (20 GB, Expect > 15 MB/s)
run_msg_size 4096 20 15.0

# 3. 6KB Sustained (45 GB, Expect > 30 MB/s)
run_msg_size 6144 45 30.0

# 4. 8KB Sustained (45 GB, Expect > 30 MB/s)
run_msg_size 8192 45 30.0

echo "=================================================" | tee -a "$LOG_DIR/suite.log"
echo " Nightly Suite Completed." | tee -a "$LOG_DIR/suite.log"
echo "=================================================" | tee -a "$LOG_DIR/suite.log"
