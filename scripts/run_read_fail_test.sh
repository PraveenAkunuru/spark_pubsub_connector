#!/bin/bash
cd "$(dirname "$0")"

PROJECT_ID="fail-test-project"
TOPIC_ID="fail-topic"
SUB_ID="non-existent-sub"

cleanup() {
  echo "Cleaning up..."
  pkill -P $$ 2>/dev/null || true
  docker stop read_fail_emulator >/dev/null 2>&1 || true
  docker rm read_fail_emulator >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup

echo "Starting Emulator..."
docker run -d --name read_fail_emulator -p 8090:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=127.0.0.1:8090

# Create Topic ONLY (so we don't fail on publishing if test does that)
# But we intentionally do NOT create the subscription.
curl -s -X PUT "http://127.0.0.1:8090/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}"

echo "Running Spark Test with MISSING Subscription..."
cd ../spark
if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
fi

export PUBSUB_PROJECT_ID="${PROJECT_ID}"
export PUBSUB_SUBSCRIPTION_ID="${SUB_ID}"
export PUBSUB_MSG_COUNT="10"
export PUBSUB_PAYLOAD_SIZE="100"
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

# We expect this to FAIL (Exit Code != 0)
if "$JAVA_HOME/bin/java" $JPMS_FLAGS -Xmx2g \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.ThroughputIntegrationTest"; then
  echo "TEST FAILED: Spark job succeeded unexpectedly!"
  exit 1
else
  echo "TEST PASSED: Spark job failed as expected."
  exit 0
fi
