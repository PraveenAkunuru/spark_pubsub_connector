#!/bin/bash
set -e
cd "$(dirname "$0")"

PROJECT_ID="spark-test-project"
TOPIC_ID="test-topic"
SUB_ID="test-sub"

cleanup() {
  echo "Cleaning up..."
  pkill -P $$ 2>/dev/null || true
  docker stop write_emulator >/dev/null 2>&1 || true
  docker rm write_emulator >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup

echo "Starting Emulator..."
docker run -d --name write_emulator -p 8089:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=localhost:8089

# Create resources
echo "Creating resources..."
curl -s -X PUT "http://localhost:8089/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"
curl -s -X PUT "http://localhost:8089/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

echo "Running PubSubWriteIntegrationTest..."
cd ../spark
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Export Env vars if needed, though these tests mostly use hardcoded values or System properties.
# WriteTest uses "spark-test-project", "test-topic" (Hardcoded).
# AckTest uses "test-project", "ack-test-topic", "ack-test-sub" (Hardcoded).

# Let's run WriteTest first.
"$JAVA_HOME/bin/java" -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.PubSubWriteIntegrationTest"

# Now run AckTest (needs its own resources)
echo "Creating AckTest resources..."
ACK_PROJECT="test-project"
ACK_TOPIC="ack-test-topic"
ACK_SUB="ack-test-sub"

curl -s -X PUT "http://localhost:8089/v1/projects/${ACK_PROJECT}/topics/${ACK_TOPIC}"
curl -s -X PUT "http://localhost:8089/v1/projects/${ACK_PROJECT}/subscriptions/${ACK_SUB}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${ACK_PROJECT}/topics/${ACK_TOPIC}\"}"

# Publish some messages for AckTest
echo "Publishing messages for AckTest..."
DATA=$(echo "test-message" | base64)
for i in {1..20}; do
  curl -s -X POST "http://localhost:8089/v1/projects/${ACK_PROJECT}/topics/${ACK_TOPIC}:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${DATA}\"}]}" > /dev/null
done

echo "Running AckIntegrationTest..."
"$JAVA_HOME/bin/java" -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.AckIntegrationTest"

echo "All verification tests passed!"
