#!/bin/bash
set -euo pipefail

# Configuration
PROJECT_ID="test-project"
TOPIC="metrics-topic"
SUB="metrics-sub"
OUTPUT_DIR="/tmp/metrics_test_output_$(date +%s)"

echo "Starting Emulator (Docker)..."
docker rm -f pubsub_metrics_test || true
docker run -d --name pubsub_metrics_test -p 8085:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=$PROJECT_ID
EMULATOR_PID=$(docker inspect --format '{{.State.Pid}}' pubsub_metrics_test)
export PUBSUB_EMULATOR_HOST=localhost:8085
sleep 10

echo "Creating Resources..."
# Create Topic
curl -s -X PUT "http://localhost:8085/v1/projects/$PROJECT_ID/topics/$TOPIC"
# Create Subscription
curl -s -X PUT "http://localhost:8085/v1/projects/$PROJECT_ID/subscriptions/$SUB" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/$PROJECT_ID/topics/$TOPIC\"}"

echo "Publishing messages..."
for i in {1..2000}; do
   # Publish enough data to trigger meaningful memory usage
   # 1KB per message * 2000 = ~2MB
   MSG_DATA=$(echo -n "{\"id\": $i, \"payload\": \"$(printf 'A%.0s' {1..1024})\"}" | base64 -w 0)
   curl -s -X POST "http://localhost:8085/v1/projects/$PROJECT_ID/topics/$TOPIC:publish" \
     -H "Content-Type: application/json" \
     -d "{\"messages\": [{\"data\": \"$MSG_DATA\"}]}" > /dev/null
done
echo "Published 2000 messages."

echo "Running Spark Job..."
export TRIGGER_INTERVAL="2 seconds"
export NUM_PARTITIONS="1"

# Using Java + sbt-launch.jar as per run_emulator_tests.sh
if [ -z "${JAVA_HOME:-}" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi
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

(cd ../spark && $JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark33/runMain com.google.cloud.spark.pubsub.test.PubSubReadToGCS $SUB $OUTPUT_DIR" > /tmp/spark_metrics_log.txt 2>&1) &
SPARK_PID=$!

echo "Monitoring for Native Metrics..."
found=0
for i in {1..20}; do
  if grep -q "Native Mem:" /tmp/spark_metrics_log.txt; then
    echo "✅ Found Metric Log:"
    grep "Native Mem:" /tmp/spark_metrics_log.txt | tail -n 5
    found=1
    break
  fi
  sleep 2
  echo -n "."
done

# Cleanup
kill $SPARK_PID || true
docker stop pubsub_metrics_test || true
rm -rf $OUTPUT_DIR

if [[ $found -eq 1 ]]; then
  echo "SUCCESS: Native Memory Metrics verified."
  exit 0
else
  echo "FAILURE: Did not find 'Native Mem:' logs."
  cat /tmp/spark_metrics_log.txt | tail -n 20
  exit 1
fi
