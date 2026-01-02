#!/bin/bash
set -e
echo "Compiling Native Rust Library..."
cd native
cargo build --release
cd ..
cp native/target/release/libnative_pubsub_connector.so libnative_pubsub_connector.so
echo "Native Library Compiled & Copied to libnative_pubsub_connector.so"
