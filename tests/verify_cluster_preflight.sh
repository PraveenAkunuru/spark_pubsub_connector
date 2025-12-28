#!/bin/bash
set -euo pipefail

# Configuration
CLUSTER_NAME="${1:-cluster-aaf3}"
REGION="${2:-us-central1}"
PROJECT_ID="${3:-pakunuru-1119-20250930202256}"
SUBSCRIPTION="${4:-benchmark-sub-1kb}"
GCS_BUCKET="pakunuru-spark-pubsub-benchmark"

echo "==========================================="
echo " Dataproc Cluster Preflight Verification"
echo "==========================================="
echo "Cluster:  $CLUSTER_NAME"
echo "Region:   $REGION"
echo "Project:  $PROJECT_ID"
echo "Sub:      $SUBSCRIPTION"
echo "Bucket:   $GCS_BUCKET"
echo "-------------------------------------------"

# 1. Check Cluster Status
echo -n "[1/4] Checking Cluster Status... "
STATUS=$(gcloud dataproc clusters describe "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT_ID" --format="value(status.state)" 2>/dev/null || echo "NOT_FOUND")
if [[ "$STATUS" == "RUNNING" ]]; then
  echo "✅ OK ($STATUS)"
else
  echo "❌ FAIL ($STATUS)"
  echo "      Hint: Is the cluster name correct? Is it started?"
  exit 1
fi

# 2. Check Service Account Scope (Indirectly via GCS)
# If the cluster is running, we assume we can submit jobs. We check if we can list the GCS bucket.
echo -n "[2/4] Checking Storage Connectivity... "
if gsutil ls -b "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
   echo "✅ OK"
else
   echo "❌ FAIL (Cannot list gs://${GCS_BUCKET})"
   echo "      Hint: Check permissions for the user/service account running this script."
   exit 1
fi

# 3. Check Pub/Sub Subscription Existence
echo -n "[3/4] Checking Pub/Sub Subscription... "
SUB_CHECK=$(gcloud pubsub subscriptions describe "$SUBSCRIPTION" --project="$PROJECT_ID" --format="value(name)" 2>/dev/null || echo "NOT_FOUND")
if [[ "$SUB_CHECK" != "NOT_FOUND" ]]; then
  echo "✅ OK"
else
  echo "❌ FAIL ('$SUBSCRIPTION' not found)"
  echo "      Hint: Did the terraform/setup script run?"
  exit 1
fi

# 4. Check Topic Association
echo -n "[4/4] Verifying Topic Association... "
TOPIC=$(gcloud pubsub subscriptions describe "$SUBSCRIPTION" --project="$PROJECT_ID" --format="value(topic)")
echo "✅ OK ($TOPIC)"

echo "-------------------------------------------"
echo "🚀 All System Checks Passed!"
echo "==========================================="
