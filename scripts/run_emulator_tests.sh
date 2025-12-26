#!/bin/bash
set -e
cd "$(dirname "$0")"

EMULATOR_PORT=8085
PROJECT_ID="spark-test-project"
TOPIC_ID="test-topic"
SUB_ID="test-sub"

# Function to cleanup emulator
cleanup() {
  echo "Stopping Pub/Sub emulator..."
  docker stop pubsub_emulator || true
  docker rm pubsub_emulator || true
}

# Trap exit signals to ensure cleanup
trap cleanup EXIT

# Start Emulator
echo "Starting Pub/Sub Emulator (Docker)..."
# 1. Start Pub/Sub emulator in Docker
cleanup
docker run -d --name pubsub_emulator -p 8085:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=spark-test-project

# Wait for emulator
echo "Waiting for emulator to be ready..."
sleep 10

export PUBSUB_EMULATOR_HOST=localhost:${EMULATOR_PORT}

# Function to create topic and sub for a project
create_resources() {
  local project=$1
  local topic=$2
  local sub=$3
  echo "Creating resources for project: $project, topic: $topic, sub: $sub"
  curl -s -X PUT "http://localhost:${EMULATOR_PORT}/v1/projects/${project}/topics/${topic}"
  if [ ! -z "$sub" ]; then
    curl -s -X PUT "http://localhost:${EMULATOR_PORT}/v1/projects/${project}/subscriptions/${sub}" \
      -H "Content-Type: application/json" \
      -d "{\"topic\": \"projects/${project}/topics/${topic}\"}"
  fi
}

# Create Resources for all Integration Tests
create_resources "spark-test-project" "test-topic" "test-sub"
create_resources "write-scale-project" "scale-topic" ""
create_resources "spark-test-project" "struct-topic" "struct-sub"
create_resources "throughput-test-project" "throughput-topic" "throughput-sub"
create_resources "scale-test-project" "scale-topic" "scale-sub"

# Publish Initial Messages for EmulatorIntegrationTest
echo "Publishing Messages to test-topic..."
for i in {1..10}; do
  MSG_DATA=$(echo -n "{\"id\": $i, \"data\": \"value_$i\"}" | base64)
  curl -s -X POST "http://localhost:${EMULATOR_PORT}/v1/projects/spark-test-project/topics/test-topic:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${MSG_DATA}\"}]}"
done

# Publish 1,000 Messages for ScaleIntegrationTest
echo "Publishing 1,000 Messages to scale-topic..."
# Using a slightly faster way to publish in batches if possible, but 1k single requests is ok for local emulator
for i in {1..1000}; do
  if (( i % 250 == 0 )); then echo "Published $i..."; fi
  MSG_DATA=$(echo -n "{\"id\": $i, \"data\": \"scale_value_$i\"}" | base64)
  curl -s -X POST "http://localhost:${EMULATOR_PORT}/v1/projects/scale-test-project/topics/scale-topic:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${MSG_DATA}\"}]}" > /dev/null
done

# Run Tests
echo "Running SBT Tests..."
# Use environment JAVA_HOME if set, otherwise default to 17
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi
echo "Using JAVA_HOME: $JAVA_HOME"

JPMS_FLAGS="--add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-opens=java.base/sun.util.logging=ALL-UNNAMED"

cd ../spark
$JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly *EmulatorIntegrationTest *StructuredReadTest"
