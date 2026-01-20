#!/bin/bash
set -euo pipefail

# ==============================================================================
# Dataproc Job Submission Template
# ==============================================================================
# This template demonstrates how to submit a Spark job to Dataproc with 
# robust logging configuration, as requested.
#
# USAGE:
#   ./submit_template.sh <CLUSTER_NAME> <REGION> <BUCKET_NAME>
# ==============================================================================

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <CLUSTER_NAME> <REGION> <BUCKET_NAME>"
    exit 1
fi

CLUSTER_NAME="$1"
REGION="$2"
BUCKET_NAME="$3"
PROJECT_ID=$(gcloud config get-value project)

# --- Logging Configuration ---
# 1. spark.eventLog.enabled=true: Enables Spark Event Logging (UI History)
#    NOTE: For high-throughput streaming with small batches, this can cause GCS rate limiting.
#          Consider disabling for benchmarks or using HDFS.
# 2. spark.eventLog.compress=true: Compresses event logs to save space/bandwidth.
# 3. spark.eventLog.rolling.enabled=true: Rolling logs (Valid for HDFS/Local, limited support on GCS).
# 4. spark.eventLog.rolling.maxFileSize=128m: Size before rolling.
# 5. log4j.threshold=WARN: Reduces driver/executor text logs to avoid noise.

PROPERTIES="\
spark.eventLog.enabled=true,\
spark.eventLog.compress=true,\
spark.eventLog.rolling.enabled=true,\
spark.eventLog.rolling.maxFileSize=128m,\
spark.driver.extraJavaOptions=-Dlog4j.threshold=WARN,\
spark.executor.extraJavaOptions=-Dlog4j.threshold=WARN"

echo "Submitting Spark Pi Job to Cluster: $CLUSTER_NAME..."

gcloud dataproc jobs submit spark \
    --cluster="$CLUSTER_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --properties="$PROPERTIES" \
    --class=org.apache.spark.examples.SparkPi \
    --jars=file:///usr/lib/spark/examples/jars/spark-examples.jar \
    -- 1000

echo "Job Submitted."
