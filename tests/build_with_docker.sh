#!/bin/bash
set -e

# Directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$DIR")"

echo "Building native library for Linux x86_64 (GLIBC 2.31 compatible) using Docker..."

docker run --rm \
  -v "$PROJECT_ROOT":/code \
  -w /code/native \
  -e CARGO_TARGET_DIR=/code/native/target_docker \
  rust:bullseye \
  cargo build --release

# Copy the artifact to the Spark resources directory
echo "Copying artifact to Spark resources..."
mkdir -p "$PROJECT_ROOT/spark/src/main/resources/linux-x86-64/"
cp "$PROJECT_ROOT/native/target_docker/release/libnative_pubsub_connector.so" \
   "$PROJECT_ROOT/spark/src/main/resources/linux-x86-64/"

echo "Build complete."
