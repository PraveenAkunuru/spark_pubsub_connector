#!/bin/bash
set -euo pipefail

CONTAINER_NAME="pubsub-emulator-test-$(date +%s)"
EMULATOR_PORT=8085

# Start Emulator via Docker
echo "Starting Pub/Sub Emulator (Docker)..."
# Pull if needed (quietly)
# docker pull gcr.io/google.com/cloudsdktool/google-cloud-cli:latest > /dev/null 2>&1 || true

docker run --rm -d \
  --name "$CONTAINER_NAME" \
  -p "$EMULATOR_PORT":8085 \
  gcr.io/google.com/cloudsdktool/google-cloud-cli:latest \
  gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085 > /tmp/pubsub-emulator-docker.log

echo "Waiting for Emulator to be ready..."
# Simple wait loop checking port
for i in {1..15}; do
  if nc -z 127.0.0.1 "$EMULATOR_PORT"; then
    echo "Emulator is up!"
    break
  fi
  sleep 1
done

export PUBSUB_EMULATOR_HOST=127.0.0.1:$EMULATOR_PORT
export PUBSUB_PROJECT_ID=test-project
export RUST_LOG=info

echo "Running Rust Integration Tests..."
cd native || exit 1

# Capture exit code
set +e
cargo test --test emulator_integration -- --nocapture
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -ne 0 ]; then
    echo " Integration Tests Failed! Docker Logs:"
    docker logs "$CONTAINER_NAME"
fi

# Cleanup
echo "Stopping Emulator..."
docker kill "$CONTAINER_NAME" > /dev/null

if [ $EXIT_CODE -eq 0 ]; then
    echo " Integration Tests Passed!"
    exit 0
else
    exit 1
fi
