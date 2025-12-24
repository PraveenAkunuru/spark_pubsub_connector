#!/bin/bash
set -e

PROJECT_ID="throughput-test-project"
TOPIC_ID="throughput-topic"
SUB_ID="throughput-sub"
MSG_COUNT=50000

echo "Starting Throughput Verification (1KB messages)..."

cleanup() {
  echo "Cleaning up emulator..."
  docker stop throughput_emulator || true
  docker rm throughput_emulator || true
}
trap cleanup EXIT

docker run -d --name throughput_emulator -p 8087:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=localhost:8087

# Create resources
curl -s -X PUT "http://localhost:8087/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"
curl -s -X PUT "http://localhost:8087/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

echo "Publishing $MSG_COUNT messages (1KB each) in batches..."
DATA_1KB=$(printf 'a%.0s' {1..1024} | base64)

for b in $(seq 1 500); do
  if [ $((b % 100)) -eq 0 ]; then echo "Batch $b/500..."; fi
  PAYLOAD_FILE="/tmp/payload_$b.json"
  echo -n "{\"messages\": [" > "$PAYLOAD_FILE"
  for i in $(seq 1 100); do
    echo -n "{\"data\": \"${DATA_1KB}\"}" >> "$PAYLOAD_FILE"
    if [ $i -lt 100 ]; then echo -n "," >> "$PAYLOAD_FILE"; fi
  done
  echo -n "]}" >> "$PAYLOAD_FILE"
  
  curl -s -X POST "http://localhost:8087/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}:publish" \
    -H "Content-Type: application/json" \
    --data-binary @"$PAYLOAD_FILE" > /dev/null
  rm "$PAYLOAD_FILE"
done

echo "Running Spark Throughput Test..."
cd spark
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
JPMS_FLAGS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

$JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.ThroughputIntegrationTest"
