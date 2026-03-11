package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds Apache Iceberg sinks that write to the S3 data lake via the
 * AWS Glue Data Catalog.
 *
 * <h3>Why Apache Iceberg?</h3>
 * <ul>
 *   <li><b>S3 today:</b> Full Parquet + partition pruning support with
 *       AWS Athena and EMR Serverless.</li>
 *   <li><b>Databricks tomorrow:</b> Databricks natively reads Iceberg
 *       tables registered in the Glue catalog (Unity Catalog external
 *       table or direct path). Migration is a catalog config change,
 *       not a data rewrite.</li>
 *   <li><b>ACID + schema evolution:</b> Iceberg V2 supports row-level
 *       deletes (for GDPR right-to-erasure), time travel, and schema
 *       evolution without full table rewrites.</li>
 * </ul>
 *
 * <h3>Partitioning</h3>
 * <p>Every table is partitioned by {@code eventDate} (truncated from
 * {@code eventTime}) plus any additional columns specified in
 * {@code streams-topology.yml}. This keeps individual files within
 * Athena's recommended 128 MB target while enabling efficient
 * date-range and domain-scoped queries.
 *
 * <h3>Databricks migration path</h3>
 * <p>When migrating to Databricks:
 * <ol>
 *   <li>Register the Glue catalog in Unity Catalog as an external catalog.</li>
 *   <li>Or add a Delta Lake sink alongside the Iceberg sink and cut over.</li>
 *   <li>All existing Parquet data files are immediately queryable —
 *       no data movement required.</li>
 * </ol>
 */
public class IcebergSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkBuilder.class);

    private final AppConfig appConfig;

    public IcebergSinkBuilder(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Attaches an Iceberg sink to {@code stream} that writes all events
     * originating from {@code streamDef} to the corresponding Iceberg table.
     *
     * <p>Call this once per stream definition, after filtering the union stream
     * to events for that stream only.
     *
     * @param stream    filtered stream of {@link RawStreamEvent} for one Kinesis topic
     * @param streamDef the stream's topology definition
     */
    public void attachSink(DataStream<RawStreamEvent> stream, StreamDefinition streamDef) {
        TableIdentifier tableId = resolveTableId(streamDef);
        TableLoader tableLoader = buildTableLoader(tableId, streamDef);

        // Convert GenericRecord stream to RowData stream expected by FlinkSink
        DataStream<GenericRecord> avroStream = stream
                .map(event -> event.getAvroRecord())
                .name("extract-avro[" + streamDef.getKey() + "]")
                .uid("extract-avro-" + streamDef.getKey());

        FlinkSink.forGenericAvroRecord()
                .table(ensureTableExists(tableId, streamDef))
                .tableLoader(tableLoader)
                .flinkConf(null) // uses env config
                .equalityFieldColumns(List.of()) // append-only (use merge for upsert)
                .overwrite(false)
                .distributionMode(org.apache.iceberg.DistributionMode.HASH)
                .writeParallelism(appConfig.getDefaultParallelism())
                .append(avroStream);

        LOG.info("Attached Iceberg sink for stream '{}' → table '{}'",
                 streamDef.getKey(), tableId);
    }

    // ── Iceberg catalog / table management ──────────────────────────────────

    private TableLoader buildTableLoader(TableIdentifier tableId, StreamDefinition streamDef) {
        CatalogLoader catalogLoader = CatalogLoader.custom(
                appConfig.getProp("iceberg.catalog.name", "prysm_glue_catalog"),
                buildCatalogProperties(),
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");

        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    /**
     * Creates the Iceberg table in Glue if it does not yet exist.
     * Schema is derived from the Avro schema registered in Glue Schema Registry.
     *
     * <p>Schema evolution (adding nullable fields) is handled automatically by
     * Iceberg's {@code updateSchema()} API on subsequent job restarts.
     */
    private Table ensureTableExists(TableIdentifier tableId, StreamDefinition streamDef) {
        Map<String, String> catalogProps = buildCatalogProperties();
        Catalog catalog = buildCatalog(catalogProps);

        if (catalog.tableExists(tableId)) {
            LOG.debug("Iceberg table '{}' already exists", tableId);
            return catalog.loadTable(tableId);
        }

        LOG.info("Creating Iceberg table '{}'", tableId);

        // Load Avro schema from classpath to derive Iceberg schema
        org.apache.avro.Schema avroSchema = loadAvroSchema(streamDef.getAvroSchema());
        Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        PartitionSpec spec = buildPartitionSpec(icebergSchema, streamDef.getPartitionBy());

        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "PARQUET");
        tableProps.put("write.parquet.compression-codec", "zstd");
        tableProps.put("write.target-file-size-bytes", "134217728"); // 128 MB
        // Enable Iceberg V2 row-level delete support (required for GDPR erasure)
        tableProps.put("format-version", "2");
        tableProps.put("write.delete.mode", "merge-on-read");
        // Metadata compaction — keeps small-files problem manageable
        tableProps.put("write.metadata.delete-after-commit.enabled", "true");
        tableProps.put("write.metadata.previous-versions-max", "5");

        return catalog.createTable(tableId, icebergSchema, spec, tableProps);
    }

    private PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionColumns) {
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        for (String col : partitionColumns) {
            if ("eventDate".equals(col)) {
                // Truncate eventTime (timestamp-ms) to day for date partitioning
                specBuilder.day("eventTime", "eventDate");
            } else if (schema.findField(col) != null) {
                specBuilder.identity(col);
            } else {
                LOG.warn("Partition column '{}' not found in Iceberg schema, skipping", col);
            }
        }
        return specBuilder.build();
    }

    private Map<String, String> buildCatalogProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        props.put(CatalogProperties.WAREHOUSE_LOCATION, appConfig.getIcebergWarehouseUri());
        props.put("glue.region", appConfig.getRegion());
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.region", appConfig.getRegion());
        // Server-side encryption with the PII KMS key for all Iceberg data files
        props.put("s3.sse.type", "aws_kms");
        props.put("s3.sse.key", appConfig.getPiiKmsKeyArn());
        return props;
    }

    private Catalog buildCatalog(Map<String, String> props) {
        org.apache.iceberg.aws.glue.GlueCatalog catalog =
                new org.apache.iceberg.aws.glue.GlueCatalog();
        catalog.initialize(
                appConfig.getProp("iceberg.catalog.name", "prysm_glue_catalog"),
                props);
        return catalog;
    }

    private TableIdentifier resolveTableId(StreamDefinition streamDef) {
        String[] parts = streamDef.getIcebergTable().split("\\.", 2);
        String dbName = appConfig.getProp("iceberg.glue.database.prefix", "prysm_")
                + parts[0] + "_" + appConfig.getEnv();
        String tableName = parts[1];
        return TableIdentifier.of(dbName, tableName);
    }

    private org.apache.avro.Schema loadAvroSchema(String schemaFileName) {
        try {
            java.io.InputStream is = getClass().getClassLoader()
                    .getResourceAsStream("avro/" + schemaFileName);
            if (is == null) {
                throw new IllegalArgumentException(
                        "Avro schema not found: avro/" + schemaFileName);
            }
            return new org.apache.avro.Schema.Parser().parse(is);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Avro schema: " + schemaFileName, e);
        }
    }
}
