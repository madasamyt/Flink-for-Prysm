# Prysm Kinesis → Flink Pipeline

Real-time streaming pipeline that ingests data from **19 AWS Kinesis streams** (customer profiles + NuSkin Prysm device health scan data), stores it in an Apache Iceberg data lake on S3, and produces streaming analytics including skin health aggregations and personalised product recommendations.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  AWS Kinesis (19 streams)                                            │
│                                                                      │
│  Customer (5)    Scan (7)         Product (3)  Commerce (2)  Device │
│  ─ profile       ─ session        ─ catalog    ─ orders      ─ telem│
│  ─ preferences   ─ facial         ─ recommends ─ returns            │
│  ─ consent       ─ hydration      ─ interactions                    │
│  ─ loyalty       ─ pigmentation                                     │
│  ─ sessions      ─ texture                                          │
│                  ─ wrinkles                                          │
│                  ─ radiance                                          │
│                  ─ overall                                           │
└────────────────────────────┬─────────────────────────────────────────┘
                             │  EFO (Enhanced Fan-Out)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Apache Flink  (3 Jobs)                                             │
│                                                                     │
│  ┌─────────────────────┐  ┌─────────────────────┐                  │
│  │  IngestionJob       │  │  ScanAnalyticsJob   │                  │
│  │  ─ PII encrypt(KMS) │  │  ─ Merge scan dims  │                  │
│  │  ─ Route to Iceberg │  │  ─ 30-min baselines │                  │
│  └─────────────────────┘  └─────────────────────┘                  │
│                                                                     │
│  ┌──────────────────────────────────┐                               │
│  │  RecommendationJob               │                               │
│  │  ─ ScanSnapshot + catalogue join │                               │
│  │  ─ Concern-to-product scoring    │                               │
│  │  ─ Purchase history dedup        │                               │
│  └──────────────────────────────────┘                               │
└──────────────────┬──────────────────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Apache Iceberg Data Lake  (S3 + AWS Glue Data Catalog)              │
│                                                                      │
│  Databases (one per domain + env):                                   │
│    prysm_customer_dev / _prod                                        │
│    prysm_scan_dev / _prod                                            │
│    prysm_product_dev / _prod                                         │
│    prysm_commerce_dev / _prod                                        │
│    prysm_device_dev / _prod                                          │
│                                                                      │
│  Query: AWS Athena  |  Future: Databricks (zero migration needed)    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### Schema Drift
All 19 Kinesis streams use **Apache Avro** schemas registered in **AWS Glue Schema Registry** with `BACKWARD` compatibility. This means:
- New optional fields (with defaults) can be added at any time without breaking consumers.
- The `metadata: map<string>` field in every schema provides a zero-schema-change escape hatch for ad-hoc additions.
- The pipeline uses `GenericRecord` (not code-generated classes) so unknown new fields pass through transparently.

### Sensitive Data Externalization

| What | Where | Why |
|------|-------|-----|
| PII field values (name, email, phone, DOB) | Encrypted in-place with AES-256-GCM, envelope-encrypted with **AWS KMS** | Field-level encryption — data is encrypted before it ever touches S3 |
| HMAC pseudonymisation salt | **AWS Secrets Manager** `/prysm/{env}/secrets/pii` | Never hardcoded; rotatable |
| KMS key ARN | **SSM Parameter Store** (SecureString) `/prysm/{env}/config/pii-kms-key-arn` | Varies per environment |
| S3 bucket names, stream names | **SSM Parameter Store** `/prysm/{env}/config/*` and `/prysm/{env}/kinesis/stream/*` | Infrastructure config stays out of source control |
| Application structure config | `streams-topology.yml` and `application.properties` | Non-sensitive; safe to commit |

### Storage: Apache Iceberg on S3
- **Today:** Query with AWS Athena, process with EMR Serverless.
- **Tomorrow (Databricks):** Register the Glue catalog in Databricks Unity Catalog as an external catalog. All Iceberg tables are immediately available — no data movement required.
- **GDPR right-to-erasure:** Iceberg V2 row-level deletes allow targeted erasure without full table rewrites.
- **Partitioning:** All tables partitioned by `eventDate` (derived from `eventTime`) + domain-specific columns.

---

## Repository Structure

```
├── pom.xml                              # Maven build (Java 11, Flink 1.18.1)
├── .env.example                         # Environment variable template
├── src/
│   ├── main/
│   │   ├── java/com/nuskin/prysm/flink/
│   │   │   ├── PrysmPipelineMain.java   # Fat-JAR entry point
│   │   │   ├── config/
│   │   │   │   ├── AppConfig.java       # Config facade (SSM + Secrets + properties)
│   │   │   │   ├── ParameterStoreUtil.java
│   │   │   │   ├── SecretsManagerUtil.java
│   │   │   │   └── StreamTopologyConfig.java  # Parses streams-topology.yml
│   │   │   ├── schema/
│   │   │   │   ├── GlueSchemaRegistryConfig.java
│   │   │   │   └── PrysmAvroDeserializationSchema.java
│   │   │   ├── source/
│   │   │   │   └── KinesisSourceBuilder.java  # EFO consumer factory
│   │   │   ├── sink/
│   │   │   │   └── IcebergSinkBuilder.java    # Glue catalog + S3 Iceberg sink
│   │   │   ├── transform/
│   │   │   │   ├── PIIEncryptionTransform.java
│   │   │   │   ├── ScanMetricsAggregator.java # Merges 7 scan dimensions per scanId
│   │   │   │   ├── RecommendationEngine.java  # Broadcast-join recommendation scoring
│   │   │   │   └── RecommendationOutput.java
│   │   │   ├── model/
│   │   │   │   ├── RawStreamEvent.java        # Pipeline envelope
│   │   │   │   └── SkinHealthSnapshot.java    # Aggregated scan result
│   │   │   └── job/
│   │   │       ├── IngestionJob.java          # 19 streams → Iceberg
│   │   │       ├── ScanAnalyticsJob.java      # Scan aggregations + windowed baselines
│   │   │       └── RecommendationJob.java     # Scan + catalogue → recommendations
│   │   └── resources/
│   │       ├── application.properties
│   │       ├── streams-topology.yml           # Stream → table mapping (no secrets)
│   │       └── avro/                          # 19 Avro schemas
│   │           ├── customer-profile.avsc
│   │           ├── ... (18 more)
│   └── test/
│       └── java/.../transform/
│           └── ScanMetricsAggregatorTest.java
└── docker/
    ├── docker-compose.yml               # LocalStack + Flink JM/TM
    └── localstack-init/
        ├── 00-run-all.sh
        ├── 01-create-kinesis-streams.sh # Creates all 19 streams
        └── 02-create-s3-buckets.sh      # Creates S3, SSM params, Secrets
```

---

## Getting Started

### Prerequisites
- Java 11+
- Maven 3.8+
- Docker + Docker Compose
- AWS CLI (for production deployments)

### Local Development

```bash
# 1. Clone and build
git clone <repo>
cd flink-prysm-pipeline
mvn clean package -DskipTests

# 2. Start LocalStack + Flink
cd docker
docker compose up -d localstack
docker compose up init          # waits for LocalStack, creates all AWS resources
docker compose up flink-jm flink-tm

# 3. Submit the ingestion job
docker compose exec flink-jm flink run \
    -c com.nuskin.prysm.flink.PrysmPipelineMain \
    /opt/flink/usrlib/flink-prysm-pipeline-1.0.0-fat.jar \
    --job ingestion

# 4. Open Flink Web UI
open http://localhost:8081

# 5. Publish test data to Kinesis
aws --endpoint-url=http://localhost:4566 kinesis put-record \
    --stream-name prysm-dev-customer-profile \
    --partition-key cust-001 \
    --data '{"customerId":"cust-001","schemaVersion":1,"eventTime":...}'
```

### Running Tests

```bash
mvn test
```

### Building for Production

```bash
mvn clean package -P prod
# Produces: target/flink-prysm-pipeline-1.0.0-fat.jar
```

---

## Adding a New Stream

1. Add an Avro schema in `src/main/resources/avro/<stream-name>.avsc`.
2. Add an entry to `streams-topology.yml` with the SSM path, Glue schema name, Iceberg table, and sensitive fields list.
3. Add the SSM parameter in each environment: `/prysm/{env}/kinesis/stream/<stream-key>`.
4. Register the Avro schema in Glue Schema Registry.
5. No code changes required — the generic pipeline picks it up automatically.

---

## Schema Evolution

To add a new optional field to an existing schema:

1. Add the field with a `default` value to the `.avsc` file.
2. Register the new schema version in Glue with `BACKWARD` compatibility — this will fail if the change is breaking.
3. Iceberg automatically evolves the table schema on the next write.
4. Old consumers continue to work (they read the default value for the new field).

---

## Databricks Migration Path

When you're ready to move to Databricks:

**Option A — Zero-copy (recommended):**
1. Register the existing AWS Glue catalog in Databricks Unity Catalog as an external catalog.
2. All Iceberg tables become immediately queryable in Databricks with no data movement.
3. Switch Flink to write to the same Iceberg tables via the Databricks-managed catalog.

**Option B — Delta Lake conversion:**
1. Add a Delta Lake sink alongside the Iceberg sink (parallel writes).
2. Validate Delta reads in Databricks.
3. Cutover Flink to write Delta only.
4. Convert using `CONVERT TO DELTA` (Parquet files are reused, no re-ingestion needed).

---

## Production Checklist

- [ ] IAM roles with least-privilege for each Flink job (separate task roles for ingestion vs recommendation)
- [ ] Glue Schema Registry `BACKWARD` compatibility enforced
- [ ] `GLUE_AUTO_REGISTER=false` in prod
- [ ] S3 bucket versioning + lifecycle policies enabled
- [ ] KMS key rotation enabled
- [ ] Flink checkpoints retained on cancellation (configured)
- [ ] PII retention policy: Iceberg time-travel + row-level deletes for GDPR erasure
- [ ] CloudWatch metrics for Flink job health, Kinesis consumer lag, Iceberg commit latency
- [ ] Separate AWS accounts (or at least VPCs) for dev/staging/prod
