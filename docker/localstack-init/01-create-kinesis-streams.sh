#!/bin/bash
# Creates all 19 Prysm Kinesis streams in LocalStack
set -euo pipefail

ENDPOINT="http://localstack:4566"
REGION="us-east-1"
SHARD_COUNT=2   # 2 shards per stream in local dev

STREAMS=(
    "prysm-dev-customer-profile"
    "prysm-dev-customer-preferences"
    "prysm-dev-customer-consent"
    "prysm-dev-customer-loyalty"
    "prysm-dev-customer-sessions"
    "prysm-dev-scan-session"
    "prysm-dev-scan-facial"
    "prysm-dev-scan-hydration"
    "prysm-dev-scan-pigmentation"
    "prysm-dev-scan-texture"
    "prysm-dev-scan-wrinkles"
    "prysm-dev-scan-radiance"
    "prysm-dev-scan-overall"
    "prysm-dev-product-catalog"
    "prysm-dev-product-recommendations"
    "prysm-dev-product-interactions"
    "prysm-dev-commerce-orders"
    "prysm-dev-commerce-returns"
    "prysm-dev-device-telemetry"
)

echo "Creating ${#STREAMS[@]} Kinesis streams..."
for STREAM in "${STREAMS[@]}"; do
    aws --endpoint-url="$ENDPOINT" --region="$REGION" \
        kinesis create-stream \
        --stream-name "$STREAM" \
        --shard-count "$SHARD_COUNT" \
        2>/dev/null || echo "  Stream $STREAM already exists"
    echo "  Created: $STREAM"
done
echo "Kinesis streams created."
