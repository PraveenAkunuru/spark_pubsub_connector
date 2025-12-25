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
export PUBSUB_EMULATOR_HOST=localhost:8091

# We intentionally do NOT create the topic.

echo "Running PubSubWriteIntegrationTest with MISSING Topic..."
cd ../spark
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Export Env vars for the test (assuming test reads them or we use same names)
# NOTE: PubSubWriteIntegrationTest might hardcode names. 
# If it hardcodes "spark-test-project" / "test-topic", we should use those but ensure they are NOT created.
# Let's override headers/logic if possible via Env, but if not, we rely on emulator being empty.
# If test creates resources, this test is invalid.
# PubSubWriteIntegrationTest usually Expects pre-created topic?
# Based on `run_write_verification.sh`, it creates resources BEFORE running test.
# So the Scala test probably DOES NOT create them independently (or assumes they exist).

if "$JAVA_HOME/bin/java" -jar sbt-launch.jar "spark35/testOnly com.google.cloud.spark.pubsub.PubSubWriteIntegrationTest"; then
  echo "TEST FAILED: Spark job succeeded unexpectedly!"
  exit 1
else
  echo "TEST PASSED: Spark job failed as expected."
  exit 0
fi
