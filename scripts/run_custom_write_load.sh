#!/bin/bash
set -e
cd "$(dirname "$0")"

# Usage: ./run_custom_write_load.sh <MSG_COUNT>
MSG_COUNT=${1:-10000}

PROJECT_ID="write-scale-project"
TOPIC_ID="scale-topic"
SUB_ID="scale-sub"

cleanup() {
  echo "Cleaning up..."
  # Kill the Java test process if it's running
  if [ -n "$TEST_PID" ]; then
    echo "Killing Java test process $TEST_PID..."
    kill -TERM "$TEST_PID" 2>/dev/null || true
    wait "$TEST_PID" 2>/dev/null || true
  fi
  
  # Kill anything spawned by this script
  pkill -P $$ 2>/dev/null || true

  echo "Cleaning up emulator..."
  docker stop write_scale_emulator >/dev/null 2>&1 || true
  docker rm write_scale_emulator >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup

echo "Starting Write Scale Emulator..."
docker run -d --name write_scale_emulator -p 8090:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 8
export PUBSUB_EMULATOR_HOST=localhost:8090

# Create resources
echo "Creating resources..."
curl -s -X PUT "http://localhost:8090/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"

# Export variables for Scala test
export PUBSUB_PROJECT_ID="${PROJECT_ID}"
export PUBSUB_TOPIC_ID="${TOPIC_ID}"
export PUBSUB_MSG_COUNT="${MSG_COUNT}"
export TEST_MASTER="${TEST_MASTER:-local[4]}"

echo "Starting Write Scalability Verification (Threads: ${TEST_MASTER}, Count: ${MSG_COUNT})..."

cd ../spark
# We use sbt to run the test
# We run in background to allow trap to catch signals
"$JAVA_HOME/bin/java" -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.WriteScaleIntegrationTest" &
TEST_PID=$!
wait $TEST_PID
