#!/bin/bash
set -euo pipefail

# verify_auth_fail.sh
# Purpose: Verify that the Spark job fails effectively when NO credentials are present.

PROJECT_ID=$(gcloud config get-value project)
SUB="metrics-sub" # Reuse existing sub
OUTPUT_DIR="/tmp/auth_fail_test_$(date +%s)"

# Ensure Java is set
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

echo "Running Spark Job WITHOUT credentials (expecting failure)..."

# Force failure by pointing to non-existent credentials
export GOOGLE_APPLICATION_CREDENTIALS="/tmp/non_existent_creds_$(date +%s).json"
unset CLOUDSDK_CONFIG

# We run locally (spark33/runMain already sets master=local[*])
# We expect exit code != 0
set +e # Allow failure
(cd ../spark && $JAVA_HOME/bin/java $JPMS_FLAGS \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -jar sbt-launch.jar "spark33/runMain com.google.cloud.spark.pubsub.test.PubSubReadToGCS $SUB $OUTPUT_DIR") > /tmp/auth_fail_log.txt 2>&1
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -eq 0 ]; then
    echo "ERROR: Job succeeded but was expected to FAIL due to missing auth."
    cat /tmp/auth_fail_log.txt
    exit 1
else
    # Check for our specific error message
    if grep -q "Fail-Fast Auth: Failed to load" /tmp/auth_fail_log.txt; then
        echo "SUCCESS: Job failed as expected with Fail-Fast Auth error."
        rm -rf $OUTPUT_DIR /tmp/auth_fail_log.txt
        exit 0
    else
        echo "ERROR: Job failed but NOT with the expected Auth error."
        head -n 20 /tmp/auth_fail_log.txt
        exit 1
    fi
fi
