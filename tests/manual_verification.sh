#!/bin/bash
set -e

PROJECT_ID="throughput-test-project"
TOPIC_ID="throughput-topic"
SUB_ID="throughput-sub"
PAYLOAD_SIZE_BYTES=1024
MSG_COUNT=5000

echo "Step 1: Cleanup"
docker rm -f custom_emulator >/dev/null 2>&1 || true

echo "Step 2: Start Emulator"
docker run -d --name custom_emulator -p 8088:8085 gcr.io/google.com/cloudsdktool/cloud-sdk:latest \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=${PROJECT_ID}

echo "Step 3: Wait for Port 8088"
set +e
for i in {1..30}; do
  if nc -z localhost 8088; then
    echo "Emulator port 8088 is open."
    break
  fi
  sleep 1
done
set -e
set -e
sleep 5

echo "Step 4: Create Resources"
export PUBSUB_EMULATOR_HOST=localhost:8088
set +e
for i in {1..5}; do
  curl -s -f -X PUT "http://localhost:8088/v1/projects/${PROJECT_ID}/topics/${TOPIC_ID}" && break
  echo "Topic creation failed/exists, retrying in 2s..."
  sleep 2
done
curl -s -f -X PUT "http://localhost:8088/v1/projects/${PROJECT_ID}/subscriptions/${SUB_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"topic\": \"projects/${PROJECT_ID}/topics/${TOPIC_ID}\"}" || echo "Sub exists or failed"
set -e

echo "Step 5: Publish messages using Python (Robust)"
python3 -c "
import base64, requests, time
data = base64.b64encode(b'a' * $PAYLOAD_SIZE_BYTES).decode()
chunk_size = 20
session = requests.Session()
max_retries = 3
for i in range(0, $MSG_COUNT, chunk_size):
    messages = [{'data': data} for _ in range(chunk_size)]
    payload = {'messages': messages}
    url = 'http://127.0.0.1:8088/v1/projects/$PROJECT_ID/topics/$TOPIC_ID:publish'
    for attempt in range(max_retries):
        try:
            r = session.post(url, json=payload, timeout=5)
            if r.status_code == 200:
                break
            print(f'Failed chunk {i} (Attempt {attempt+1}): {r.status_code}')
        except Exception as e:
            print(f'Error chunk {i} (Attempt {attempt+1}): {e}')
        time.sleep(2)
    else:
        print(f'Failed to publish chunk {i} after {max_retries} attempts')
        exit(1)
    if (i % 500) == 0:
        print(f'Progress: {i}/{ $MSG_COUNT } messages published')
"

echo "Step 6: Run Spark Test (Local Mode)"
cd /usr/local/google/home/pakunuru/spark_pubsub_connector/spark
export PUBSUB_PROJECT_ID="${PROJECT_ID}"
export PUBSUB_SUBSCRIPTION_ID="${SUB_ID}"
export PUBSUB_MSG_COUNT="${MSG_COUNT}"
export PUBSUB_PAYLOAD_SIZE="${PAYLOAD_SIZE_BYTES}"
export TEST_MASTER="local[4]" # Use local[4] to reduce contention
export PUBSUB_NUM_PARTITIONS="2"
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
JPMS_FLAGS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
export LD_LIBRARY_PATH="/usr/local/google/home/pakunuru/spark_pubsub_connector/native/target/release:$LD_LIBRARY_PATH"

$JAVA_HOME/bin/java $JPMS_FLAGS -Xmx4g \
    -Dorg.apache.arrow.memory.util.MemoryUtil.DISABLE_UNSAFE_DIRECT_MEMORY_ACCESS=false \
    -Djava.library.path="/usr/local/google/home/pakunuru/spark_pubsub_connector/native/target/release" \
    -jar sbt-launch.jar "testOnly finalconnector.ThroughputIntegrationTest" 2>&1 | tee ../logs/verification_run.log
