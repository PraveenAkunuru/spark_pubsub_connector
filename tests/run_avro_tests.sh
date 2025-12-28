#!/bin/bash
set -e
cd "$(dirname "$0")"

EMULATOR_PORT=8686
PROJECT_ID="spark-test-project"
TOPIC_ID="avro-test-topic"
SUB_ID="avro-test-sub"

# Function to cleanup emulator
cleanup() {
  echo "Stopping Pub/Sub emulator..."
  docker stop pubsub_emulator || true
  docker rm pubsub_emulator || true
}

# Trap exit signals
trap cleanup EXIT

# 1. Start Pub/Sub emulator
echo "Starting Pub/Sub Emulator..."
cleanup
docker run -d --name pubsub_emulator -p ${EMULATOR_PORT}:${EMULATOR_PORT} gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:${EMULATOR_PORT} --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10

export PUBSUB_EMULATOR_HOST=127.0.0.1:${EMULATOR_PORT}

# 2. Create Topic
echo "Creating Topic $TOPIC_ID..."
curl -s -X PUT "http://127.0.0.1:${EMULATOR_PORT}/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"

# 3. Create Subscription
echo "Creating Subscription $SUB_ID..."
curl -s -X PUT "http://127.0.0.1:${EMULATOR_PORT}/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

# 4. Run Scala Test
echo "Running AvroIntegrationTest..."
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi

export RUST_LOG=info

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
  --add-opens=java.base/sun.util.logging=ALL-UNNAMED \
  --add-opens=java.base/javax.security.auth=ALL-UNNAMED"

cd ../spark
$JAVA_HOME/bin/java $JPMS_FLAGS -Xmx2g \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -Dpubsub.emulator.host=127.0.0.1:${EMULATOR_PORT} \
    -jar sbt-launch.jar spark35/clean spark35/update "spark35/testOnly com.google.cloud.spark.pubsub.AvroIntegrationTest"
