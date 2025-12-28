#!/bin/bash
set -e
cd "$(dirname "$0")"

EMULATOR_PORT=8085
export PUBSUB_EMULATOR_HOST=localhost:${EMULATOR_PORT}

echo "Using existing Pub/Sub Emulator at ${PUBSUB_EMULATOR_HOST}"

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
create_resources "spark-test-project" "test-topic-1" "test-sub-1"
create_resources "spark-test-project" "test-topic-2" "test-sub-2"
create_resources "write-scale-project" "scale-topic" ""
create_resources "spark-test-project" "struct-topic" "struct-sub"
create_resources "throughput-test-project" "throughput-topic" "throughput-sub"
create_resources "scale-test-project" "scale-topic" "scale-sub"

# Publish Messages for Test 1
echo "Publishing Messages to test-topic-1..."
for i in {1..10}; do
  MSG_DATA=$(echo -n "{\"id\": $i, \"data\": \"value_$i\"}" | base64)
  curl -s -X POST "http://localhost:${EMULATOR_PORT}/v1/projects/spark-test-project/topics/test-topic-1:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${MSG_DATA}\"}]}" > /dev/null
done

# Publish Messages for Test 2
echo "Publishing Messages to test-topic-2..."
for i in {1..10}; do
  MSG_DATA=$(echo -n "{\"id\": $i, \"data\": \"parity_$i\"}" | base64)
  curl -s -X POST "http://localhost:${EMULATOR_PORT}/v1/projects/spark-test-project/topics/test-topic-2:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${MSG_DATA}\"}]}" > /dev/null
done

# Publish 1,000 Messages for ScaleIntegrationTest
echo "Publishing 1,000 Messages to scale-topic..."
for i in {1..1000}; do
  if (( i % 250 == 0 )); then echo "Published $i..."; fi
  MSG_DATA=$(echo -n "{\"id\": $i, \"data\": \"scale_value_$i\"}" | base64)
  curl -s -X POST "http://localhost:${EMULATOR_PORT}/v1/projects/scale-test-project/topics/scale-topic:publish" \
    -H "Content-Type: application/json" \
    -d "{\"messages\": [{\"data\": \"${MSG_DATA}\"}]}" > /dev/null
done

# Run Tests
echo "Running SBT Tests..."
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
