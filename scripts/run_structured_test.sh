#!/bin/bash
set -e
cd "$(dirname "$0")"

EMULATOR_PORT=8085
PROJECT_ID="spark-test-project"
TOPIC_ID="struct-topic"
SUB_ID="struct-sub"

# Function to cleanup emulator
cleanup() {
  echo "Stopping Pub/Sub emulator..."
  docker stop pubsub_emulator_struct || true
  docker rm pubsub_emulator_struct || true
}

# Trap exit signals to ensure cleanup
trap cleanup EXIT

# Start Emulator
echo "Starting Pub/Sub Emulator (Docker)..."
cleanup
docker run -d --name pubsub_emulator_struct -p 8085:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

# Wait for emulator
echo "Waiting for emulator to be ready..."
sleep 10

export PUBSUB_EMULATOR_HOST=localhost:${EMULATOR_PORT}

# Create Topic
echo "Creating Topic ${TOPIC_ID}..."
curl -s -X PUT "http://localhost:${EMULATOR_PORT}/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"

# Create Subscription
echo "Creating Subscription ${SUB_ID}..."
curl -s -X PUT "http://localhost:${EMULATOR_PORT}/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

# Run Tests
echo "Running StructuredReadTest..."
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
export RUST_LOG=info
$JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.StructuredReadTest"
