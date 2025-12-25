#!/bin/bash
cd "$(dirname "$0")"

PROJECT_ID="write-fail-project"
TOPIC_ID="non-existent-topic"

cleanup() {
  echo "Cleaning up..."
  pkill -P $$ 2>/dev/null || true
  docker stop write_fail_emulator >/dev/null 2>&1 || true
  docker rm write_fail_emulator >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup

echo "Starting Emulator..."
docker run -d --name write_fail_emulator -p 8091:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Waiting for emulator..."
sleep 10
export PUBSUB_EMULATOR_HOST=127.0.0.1:8091

# We intentionally do NOT create the topic.

echo "Running PubSubWriteIntegrationTest with MISSING Topic..."
cd ../spark
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

if "$JAVA_HOME/bin/java" $JPMS_FLAGS -Xmx2g \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.PubSubWriteIntegrationTest"; then
  echo "TEST FAILED: Spark job succeeded unexpectedly!"
  exit 1
else
  echo "TEST PASSED: Spark job failed as expected."
  exit 0
fi
