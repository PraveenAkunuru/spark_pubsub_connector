#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "==================================================="
echo "Starting Write Scalability Test (2 -> 10 -> 2)"
echo "==================================================="

clean_up() {
    echo "Terminating write scaling test..."
    pkill -P $$ 2>/dev/null || true
}
trap clean_up EXIT INT TERM

RUN_TEST() {
  local MASTER=$1
  local DESC=$2
  echo ">>> Running Write Test: $DESC (Master: $MASTER) <<<"
  
  export TEST_MASTER=$MASTER
  
  # 10k messages per run
  ./run_custom_write_load.sh 10000
  
  echo ">>> Test $DESC Completed <<<"
  sleep 5
}

# 1. Start with 2 executors
RUN_TEST "local[2]" "Initial Scale (2 Threads)"

# 2. Scale up to 10 executors
RUN_TEST "local[10]" "Scaled Up (10 Threads)"

# 3. Scale down to 2 executors
RUN_TEST "local[2]" "Scaled Down (2 Threads)"

echo "Write Scalability Test Complete."
