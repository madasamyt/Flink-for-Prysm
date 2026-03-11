#!/bin/bash
# Creates S3 buckets and registers SSM parameters for the local dev environment
set -euo pipefail

ENDPOINT="http://localstack:4566"
REGION="us-east-1"
DATA_LAKE_BUCKET="prysm-dev-data-lake"
CHECKPOINT_BUCKET="prysm-dev-flink-checkpoints"

echo "Creating S3 buckets..."
for BUCKET in "$DATA_LAKE_BUCKET" "$CHECKPOINT_BUCKET"; do
    aws --endpoint-url="$ENDPOINT" --region="$REGION" \
        s3api create-bucket --bucket "$BUCKET" \
        2>/dev/null || echo "  Bucket $BUCKET already exists"
    echo "  Created: s3://$BUCKET"
done

# Enable versioning on data lake bucket (required for Iceberg ACID)
aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    s3api put-bucket-versioning \
    --bucket "$DATA_LAKE_BUCKET" \
    --versioning-configuration Status=Enabled

echo "S3 buckets created."

# ── SSM Parameters ────────────────────────────────────────────────────────────
echo "Registering SSM parameters..."

SSM_PARAMS=(
    "/prysm/local/config/data-lake-bucket:$DATA_LAKE_BUCKET"
    "/prysm/local/config/checkpoint-bucket:$CHECKPOINT_BUCKET"
    "/prysm/local/config/pii-kms-key-arn:arn:aws:kms:us-east-1:000000000000:key/local-dev-key"
    # Stream names
    "/prysm/local/kinesis/stream/customer-profile:prysm-dev-customer-profile"
    "/prysm/local/kinesis/stream/customer-preferences:prysm-dev-customer-preferences"
    "/prysm/local/kinesis/stream/customer-consent:prysm-dev-customer-consent"
    "/prysm/local/kinesis/stream/customer-loyalty:prysm-dev-customer-loyalty"
    "/prysm/local/kinesis/stream/customer-sessions:prysm-dev-customer-sessions"
    "/prysm/local/kinesis/stream/scan-session:prysm-dev-scan-session"
    "/prysm/local/kinesis/stream/scan-facial:prysm-dev-scan-facial"
    "/prysm/local/kinesis/stream/scan-hydration:prysm-dev-scan-hydration"
    "/prysm/local/kinesis/stream/scan-pigmentation:prysm-dev-scan-pigmentation"
    "/prysm/local/kinesis/stream/scan-texture:prysm-dev-scan-texture"
    "/prysm/local/kinesis/stream/scan-wrinkles:prysm-dev-scan-wrinkles"
    "/prysm/local/kinesis/stream/scan-radiance:prysm-dev-scan-radiance"
    "/prysm/local/kinesis/stream/scan-overall:prysm-dev-scan-overall"
    "/prysm/local/kinesis/stream/product-catalog:prysm-dev-product-catalog"
    "/prysm/local/kinesis/stream/product-recommendations:prysm-dev-product-recommendations"
    "/prysm/local/kinesis/stream/product-interactions:prysm-dev-product-interactions"
    "/prysm/local/kinesis/stream/commerce-orders:prysm-dev-commerce-orders"
    "/prysm/local/kinesis/stream/commerce-returns:prysm-dev-commerce-returns"
    "/prysm/local/kinesis/stream/device-telemetry:prysm-dev-device-telemetry"
)

for PARAM in "${SSM_PARAMS[@]}"; do
    NAME="${PARAM%%:*}"
    VALUE="${PARAM#*:}"
    aws --endpoint-url="$ENDPOINT" --region="$REGION" \
        ssm put-parameter \
        --name "$NAME" \
        --value "$VALUE" \
        --type "String" \
        --overwrite \
        > /dev/null
    echo "  SSM: $NAME = $VALUE"
done

# ── Secrets Manager ───────────────────────────────────────────────────────────
echo "Registering Secrets Manager secrets..."
aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    secretsmanager create-secret \
    --name "/prysm/local/secrets/pii" \
    --secret-string '{"hmacSecret":"local-dev-hmac-salt-change-in-prod"}' \
    2>/dev/null || \
aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    secretsmanager put-secret-value \
    --secret-id "/prysm/local/secrets/pii" \
    --secret-string '{"hmacSecret":"local-dev-hmac-salt-change-in-prod"}'
echo "  Secret /prysm/local/secrets/pii created"

# ── KMS key ───────────────────────────────────────────────────────────────────
echo "Creating KMS key for PII encryption..."
KEY_ID=$(aws --endpoint-url="$ENDPOINT" --region="$REGION" \
    kms create-key \
    --description "Prysm dev PII encryption key" \
    --query 'KeyMetadata.KeyId' \
    --output text 2>/dev/null || echo "existing")
echo "  KMS key: $KEY_ID"

echo "All AWS resources initialised for local dev."
