#!/bin/bash
set -e
cd "$(dirname "$0")"

PROJECT_ID="scale-test-project"
TOPIC_ID="scale-topic"
SUB_ID="scale-sub"
MSG_COUNT=10000
PARTITIONS=10

echo "Starting Scale Verification..."

cleanup() {
  echo "Cleaning up emulator..."
  docker stop scale_emulator || true
  docker rm scale_emulator || true
}
trap cleanup EXIT

docker run -d --name scale_emulator -p 8086:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=127.0.0.1:8086

# Create resources
curl -s -X PUT "http://127.0.0.1:8086/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"
curl -s -X PUT "http://127.0.0.1:8086/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}"

echo "Publishing $MSG_COUNT messages in batches..."
for b in $(seq 1 100); do
  MESSAGES=""
  for i in $(seq 1 100); do
    ID=$(( (b-1)*100 + i ))
    DATA=$(echo -n "{\"id\": $ID, \"data\": \"scale_data_${ID}\"}" | base64)
    MESSAGES="${MESSAGES}{\"data\": \"${DATA}\"},"
  done
  MESSAGES=${MESSAGES%?} # Remove trailing comma
  curl -s -X POST "http://127.0.0.1:8086/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [${MESSAGES}]}" > /dev/null
done
# This does 10,000 messages (100 batches of 100)

echo "Running Spark Scale Test..."
cd ../spark
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
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

$JAVA_HOME/bin/java $JPMS_FLAGS -Xmx2g \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.ScaleIntegrationTest"
