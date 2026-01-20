#!/bin/bash
set -euo pipefail

# ==============================================================================
# Spark Pub/Sub Connector - Matrix Benchmark Suite (Optimized)
# ==============================================================================

PROJECT_ID="pakunuru-1119-20250930202256"
CLUSTER="cluster-be84"
BUCKET="gs://pakunuru-spark-pubsub-benchmark"
VOLUME_GB=5
MSG_SIZES=(2048 4096 6144 8192)
ITERATIONS=1
EXECUTORS=2 # Reduced to 2 to ensure it fits (Total cores used ~5-6)

REPO_ROOT="$(dirname "$(dirname "$(dirname "$(realpath "$0")")")")"
LOG_DIR="$REPO_ROOT/logs/matrix_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"
REPORT_FILE="$LOG_DIR/throughput_matrix_report.csv"

echo "Timestamp,Message Size (Bytes),Iteration,Batch Size,Throughput (MB/s),Throughput (Msg/s),Throughput/Core (MB/s),Throughput/Core (Msg/s),Total Cores,Driver CPU %,Driver Heap,Cluster RSS (MB),Cluster Heap (MB)" > "$REPORT_FILE"

echo "================================================="
echo " Starting Benchmark Matrix (Safe Mode)"
echo " Cluster Capacity Limited"
echo " Batch Payload Limit: < 8MB"
echo "================================================="

# Static Metrics
TOTAL_EXECUTORS=$EXECUTORS
CORES_PER_EXECUTOR=2
TOTAL_CORES=$((TOTAL_EXECUTORS * CORES_PER_EXECUTOR))
SYSTEM_CPU_EST="N/A"
SYSTEM_MEM_EST="4g/Executor"

for MSG_SIZE in "${MSG_SIZES[@]}"; do
    
    # Calculate Safe Batch Sizes (< 8MB payload)
    # 8MB = 8 * 1024 * 1024 = 8388608 bytes
    LIMIT_BYTES=8000000 
    MAX_BATCH=$((LIMIT_BYTES / MSG_SIZE))
    
    # Define interesting batch steps relative to max
    # Low (25%), Med (50%), High (90%)
    B1=$(python3 -c "print(int($MAX_BATCH * 0.25))")
    B2=$(python3 -c "print(int($MAX_BATCH * 0.50))")
    B3=$(python3 -c "print(int($MAX_BATCH * 0.90))")
    
    BATCH_SIZES=($B1 $B2 $B3)
    # Filter out 0 or too small
    BATCH_SIZES=($(printf "%s\n" "${BATCH_SIZES[@]}" | awk '$1 > 10'))
    
    echo "----------------------------------------------------------------"
    echo " Size: ${MSG_SIZE}b | Safe Batches: ${BATCH_SIZES[*]} | Limit: ${MAX_BATCH}"
    echo "----------------------------------------------------------------"
    
    # Phase 1: Generate Data (Once per size)
    REPLAY_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    echo "Capturing Replay Time: $REPLAY_TIME"
    sleep 3 # Ensure time gap
    
    timeout 3600 $REPO_ROOT/scripts/benchmark/run_throughput_suite.sh \
        --project "$PROJECT_ID" \
        --cluster "$CLUSTER" \
        --bucket "$BUCKET" \
        --msg-size "$MSG_SIZE" \
        --volume-gb "$VOLUME_GB" \
        --executors "$EXECUTORS" \
        --mode generate | tee "$LOG_DIR/gen_${MSG_SIZE}b.log" || true
        
    if grep -q "ERROR" "$LOG_DIR/gen_${MSG_SIZE}b.log"; then
        echo "Generation Failed. Skipping..."
        continue
    fi

    # Phase 2: Read Benchmarks
    for BATCH_SIZE in "${BATCH_SIZES[@]}"; do
        for ((ITER=1; ITER<=ITERATIONS; ITER++)); do
            RUN_LOG="$LOG_DIR/run_${MSG_SIZE}b_batch${BATCH_SIZE}_iter${ITER}.log"
            
            timeout 3600 $REPO_ROOT/scripts/benchmark/run_throughput_suite.sh \
                --project "$PROJECT_ID" \
                --cluster "$CLUSTER" \
                --bucket "$BUCKET" \
                --msg-size "$MSG_SIZE" \
                --volume-gb "$VOLUME_GB" \
                --batch-size "$BATCH_SIZE" \
                --executors "$EXECUTORS" \
                --seek-time "$REPLAY_TIME" \
                --mode read | tee "$RUN_LOG" || true
            
            MB_SEC=$(grep "Avg Throughput:" "$RUN_LOG" | tail -n 1 | awk '{print $5}' | tr -d '(')
            MSG_SEC=$(grep "Avg Throughput:" "$RUN_LOG" | tail -n 1 | awk '{print $3}')
            
            # Parse Driver Metrics
            DRV_CPU=$(grep "Driver CPU Load:" "$RUN_LOG" | tail -n 1 | awk '{print $4}')
            DRV_MEM=$(grep "Driver Heap Used:" "$RUN_LOG" | tail -n 1 | awk '{print $4, $5, $6}') # e.g. "123MB / 4096MB"
            
            # Parse Cluster Metrics
            CLUS_RSS=$(grep "Cluster Total RSS:" "$RUN_LOG" | tail -n 1 | awk '{print $4}' | tr -d 'MB')
            CLUS_HEAP=$(grep "Cluster Total Heap:" "$RUN_LOG" | tail -n 1 | awk '{print $4}' | tr -d 'MB')
            
            if [[ -z "$MSG_SEC" ]]; then MSG_SEC="0"; MB_SEC="0"; fi
            if [[ -z "$DRV_CPU" ]]; then DRV_CPU="N/A"; fi
            if [[ -z "$DRV_MEM" ]]; then DRV_MEM="N/A"; fi
            if [[ -z "$CLUS_RSS" ]]; then CLUS_RSS="N/A"; fi
            if [[ -z "$CLUS_HEAP" ]]; then CLUS_HEAP="N/A"; fi
            
            PER_CORE_MSG=$(echo "$MSG_SEC / $TOTAL_CORES" | bc 2>/dev/null || echo "0")
            PER_CORE_MB=$(echo "$MB_SEC / $TOTAL_CORES" | bc 2>/dev/null || echo "0")
            
            TS=$(date "+%Y-%m-%d %H:%M:%S")
            echo "${TS},${MSG_SIZE},${ITER},${BATCH_SIZE},${MB_SEC},${MSG_SEC},${PER_CORE_MB},${PER_CORE_MSG},${TOTAL_CORES},${DRV_CPU},${DRV_MEM},${CLUS_RSS},${CLUS_HEAP}" >> "$REPORT_FILE"
            sleep 10
        done
    done
done

cat "$REPORT_FILE"
