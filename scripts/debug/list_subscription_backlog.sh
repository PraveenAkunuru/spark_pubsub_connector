#!/bin/bash
set -u

# ==============================================================================
# Helper to fetch Pub/Sub Subscription Metrics
# ==============================================================================

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <PROJECT_ID> <SUBSCRIPTION_NAME>"
    exit 1
fi

PROJECT="$1"
SUB_NAME="$2"

echo "Fetching metrics for subscription: events/$SUB_NAME in project $PROJECT..."

# Define Time Window (Last 1 Hour)
START_TIME=$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)
END_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Metric: subscription/num_undelivered_messages
# Resource Label: subscription_id

FILTER="metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id=\"$SUB_NAME\""

echo "Querying Monitoring API..."
echo "Filter: $FILTER"
echo "Interval: $START_TIME to $END_TIME"

gcloud monitoring time-series list \
    --project="$PROJECT" \
    --filter="$FILTER" \
    --interval="start_time=$START_TIME,end_time=$END_TIME" \
    --aggregation="alignmentPeriod=60s,perSeriesAligner=ALIGN_MAX" \
    --format="table(points.value.int64Value:label=BACKLOG_COUNT, points.interval.endTime:label=TIME)"

echo "Done."
