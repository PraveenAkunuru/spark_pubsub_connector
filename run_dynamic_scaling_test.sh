#!/bin/bash
set -e

# Wraps run_custom_throughput.sh but overrides SPARK_MASTER for each run.
# Since run_custom_throughput.sh calls sbt/spark directly, we need to modify it or ThroughputIntegrationTest.scala to use SPARK_MASTER env var.
# I updated ThroughputIntegrationTest.scala to use `sys.props` but not `SPARK_MASTER`.
# Actually, I didn't update `ThroughputIntegrationTest.scala` to read `spark.master`.
# It still has `.master("local[4]")`.
# I MUST update `ThroughputIntegrationTest.scala` first to allow master override.
# I'll do that in the next step.

# This script assumes ThroughputIntegrationTest.scala reads `spark.master` property.

echo "==================================================="
echo "Starting Dynamic Scaling Simulation (2 -> 10 -> 2)"
echo "==================================================="

RUN_TEST() {
  local MASTER=$1
  local DESC=$2
  echo ">>> Running Test: $DESC (Master: $MASTER) <<<"
  
  # We use the same payload/count for consistency (e.g. 1KB, 10k msgs)
  # ./run_custom_throughput.sh 1024 10000
  # But we need to pass the MASTER. 
  # run_custom_throughput.sh calls java ... -jar sbt-launch.jar ...
  # We can export SPARK_MASTER_PROP for Scala to pick up?
  # Scala accesses `sys.props`.
  # We can pass `-Dspark.master=$MASTER` to the java command in run_custom_throughput.sh?
  # run_custom_throughput.sh doesn't accept extra args.
  # I will modify run_custom_throughput.sh to accept EXTRA_JAVA_OPTS or similar.
  # Or I can just manually invoke the command here.
  
  export PUBSUB_PAYLOAD_SIZE=1024
  export PUBSUB_MSG_COUNT=10000
  export TEST_MASTER=$MASTER
  
  # Invoke the test directly (skipping run_custom_throughput.sh wrapper or modifying it?)
  # It's better to modify run_custom_throughput.sh to respect `TEST_MASTER` env var if set.
  # For now, I'll rely on my upcoming modification to run_custom_throughput.sh
  
  ./run_custom_throughput.sh 1024 10000
  
  echo ">>> Test $DESC Completed <<<"
  sleep 5
}

# 1. Start with 2 executors (threads)
export TEST_MASTER="local[2]"
RUN_TEST "local[2]" "Initial Scale (2 Threads)"

# 2. Scale up to 10 executors
export TEST_MASTER="local[10]"
RUN_TEST "local[10]" "Scaled Up (10 Threads)"

# 3. Scale down to 2 executors
export TEST_MASTER="local[2]"
RUN_TEST "local[2]" "Scaled Down (2 Threads)"

echo "Dynamic Scaling Simulation Complete."
