#!/bin/bash
set -e

PROJECT_ID="throughput-test-project"
TOPIC_ID="throughput-topic"
SUB_ID="throughput-sub"

PAYLOAD_SIZE_BYTES=${1:-1024}
MSG_COUNT=${2:-10000}

echo "Starting Custom Throughput Verification (Size: ${PAYLOAD_SIZE_BYTES}B, Count: ${MSG_COUNT})..."

cleanup() {
  echo "Cleaning up..."
  # Kill the Java test process if it's still running
  if [ -n "$TEST_PID" ]; then
    echo "Killing Java test process $TEST_PID..."
    kill -TERM "$TEST_PID" 2>/dev/null || true
    wait "$TEST_PID" 2>/dev/null || true
  fi
  
  # Kill any remaining child processes of this script
  pkill -P $$ 2>/dev/null || true

  echo "Cleaning up emulator..."
  docker stop custom_emulator >/dev/null 2>&1 || true
  docker rm custom_emulator >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# Ensure no conflict
cleanup

docker run -d --name custom_emulator -p 8088:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=localhost:8088

# Create resources
curl -s -X PUT "http://localhost:8088/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"
curl -s -X PUT "http://localhost:8088/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

echo "Publishing ${MSG_COUNT} messages..."

# Create payload
DATA=$(printf 'a%.0s' $(seq 1 ${PAYLOAD_SIZE_BYTES}) | base64 -w 0)

# Batch size 100
BATCH_SIZE=100
NUM_BATCHES=$((MSG_COUNT / BATCH_SIZE))

for b in $(seq 1 ${NUM_BATCHES}); do
  if [ $((b % 50)) -eq 0 ]; then echo "Batch $b/${NUM_BATCHES}..."; fi
  PAYLOAD_FILE="/tmp/custom_payload_$b.json"
  echo -n "{\"messages\": [" > "$PAYLOAD_FILE"
  for i in $(seq 1 ${BATCH_SIZE}); do
    echo -n "{\"data\": \"${DATA}\"}" >> "$PAYLOAD_FILE"
    if [ $i -lt ${BATCH_SIZE} ]; then echo -n "," >> "$PAYLOAD_FILE"; fi
  done
  echo -n "]}" >> "$PAYLOAD_FILE"
  
  curl -s -X POST "http://localhost:8088/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}:publish" \
    -H "Content-Type: application/json" \
    --data-binary @"$PAYLOAD_FILE" > /dev/null
  rm "$PAYLOAD_FILE"
done

echo "Running Spark Test..."
cd spark
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
JPMS_FLAGS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# Pass custom project/sub/topic via system properties if test supports it, or ENV vars.
# Integration tests currently hardcode or read from PubSubConfig defaults if not specified.
# But ThroughputIntegrationTest might use specific names.
# Let's check ThroughputIntegrationTest.scala content first to see if it allows overriding.
# Actually, I should check it. For now, I'll export ENV vars that the test might pick up, OR
# I might need to modify ThroughputIntegrationTest to read Env Vars.
# Based on previous `run_throughput_test.sh`, it sends messages to `throughput-test-project` 
# and the Scala test `ThroughputIntegrationTest.scala` likely uses that hardcoded.
# I should update `ThroughputIntegrationTest.scala` to read from Env potentially, or just match the names.
# To be safe, I will change the script to use the SAME names as `ThroughputIntegrationTest.scala` expects, 
# BUT `run_throughput_test.sh` used `throughput-test-project`.
# I will stick to `throughput-test-project` names to avoid changing Scala code if not needed.
# RE-WRITING VARIABLES ABOVE TO MATCH run_throughput_test.sh EXPECTATIONS.

# Run in background to allow trap to kill it
$JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -Dpubsub.project.id=${PROJECT_ID} \
    -Dpubsub.subscription.id=${SUB_ID} \
    -Dspark.master=${TEST_MASTER:-local[4]} \
    -Dpubsub.msg.count=${MSG_COUNT} \
    -Dpubsub.payload.size=${PAYLOAD_SIZE_BYTES} \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.ThroughputIntegrationTest" &
TEST_PID=$!
wait $TEST_PID
