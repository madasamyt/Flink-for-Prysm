package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.CatalogType;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.IcebergSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.WriteMode;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds Apache Iceberg sinks driven by the {@link IcebergSinkConfig} metadata
 * declared in {@code streams-topology.yml}.
 *
 * <h3>Catalog support</h3>
 * <p>The {@code sinks[].catalogType} field selects the catalog implementation:
 * <ul>
 *   <li>{@code GLUE} (default) — AWS Glue Data Catalog; fully managed,
 *       compatible with Athena, EMR, Databricks Unity Catalog</li>
 *   <li>{@code NESSIE} — Project Nessie REST catalog; git-like branching for
 *       schema migrations and data experiments; set {@code nessieEndpoint} and
 *       optionally {@code targetBranch} in the sink config</li>
 *   <li>{@code HIVE} — Apache Hive Metastore; for on-premise or non-AWS
 *       deployments</li>
 * </ul>
 *
 * <h3>Write modes</h3>
 * <ul>
 *   <li>{@code APPEND} (default) — insert-only; optimal for event streams</li>
 *   <li>{@code UPSERT} — requires {@code upsertKey} in the sink config;
 *       uses Iceberg V2 merge-on-read for CDC and slowly-changing dimensions</li>
 * </ul>
 */
public class IcebergSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkBuilder.class);

    private final AppConfig appConfig;

    public IcebergSinkBuilder(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    /**
     * Attaches an Iceberg sink to {@code stream} using the metadata from
     * {@code icebergCfg}.  Call once per ICEBERG sink entry in the stream's
     * {@code sinks[]} list.
     */
    public void attachSink(DataStream<RawStreamEvent> stream,
                            StreamDefinition streamDef,
                            IcebergSinkConfig icebergCfg) {
        TableIdentifier tableId = resolveTableId(icebergCfg);
        TableLoader tableLoader = buildTableLoader(tableId, icebergCfg);

        DataStream<GenericRecord> avroStream = stream
                .map(event -> event.getAvroRecord())
                .name("extract-avro[" + streamDef.getKey() + "]")
                .uid("extract-avro-" + streamDef.getKey());

        List<String> equalityFields = new ArrayList<>();
        if (icebergCfg.getWriteMode() == WriteMode.UPSERT
                && icebergCfg.getUpsertKey() != null) {
            equalityFields.add(icebergCfg.getUpsertKey());
        }

        FlinkSink.forGenericAvroRecord()
                .table(ensureTableExists(tableId, streamDef, icebergCfg))
                .tableLoader(tableLoader)
                .flinkConf(null)
                .equalityFieldColumns(equalityFields)
                .overwrite(false)
                .distributionMode(org.apache.iceberg.DistributionMode.HASH)
                .writeParallelism(appConfig.getDefaultParallelism())
                .append(avroStream);

        LOG.info("Attached Iceberg sink for stream '{}' → table '{}' (mode={}, catalog={})",
                 streamDef.getKey(), tableId,
                 icebergCfg.getWriteMode(), icebergCfg.getCatalogType());
    }

    /**
     * Backward-compatible overload used by existing code that calls
     * {@code attachSink(stream, streamDef)} without an explicit IcebergSinkConfig.
     * Delegates to the primary Iceberg sink in the stream definition.
     */
    public void attachSink(DataStream<RawStreamEvent> stream, StreamDefinition streamDef) {
        IcebergSinkConfig icebergCfg = streamDef.primaryIcebergSink();
        if (icebergCfg == null) {
            throw new IllegalStateException(
                    "No ICEBERG sink configured for stream '" + streamDef.getKey() + "'");
        }
        attachSink(stream, streamDef, icebergCfg);
    }

    // ── Iceberg catalog / table management ───────────────────────────────────

    private TableLoader buildTableLoader(TableIdentifier tableId,
                                          IcebergSinkConfig icebergCfg) {
        CatalogLoader catalogLoader = buildCatalogLoader(icebergCfg);
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    private Table ensureTableExists(TableIdentifier tableId,
                                     StreamDefinition streamDef,
                                     IcebergSinkConfig icebergCfg) {
        Map<String, String> catalogProps = buildCatalogProperties(icebergCfg);
        Catalog catalog = buildCatalog(icebergCfg, catalogProps);

        if (catalog.tableExists(tableId)) {
            LOG.debug("Iceberg table '{}' already exists", tableId);
            return catalog.loadTable(tableId);
        }

        LOG.info("Creating Iceberg table '{}'", tableId);

        org.apache.avro.Schema avroSchema = loadAvroSchema(streamDef.getAvroSchema());
        Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        PartitionSpec spec = buildPartitionSpec(
                icebergSchema, icebergCfg.getPartitionColumns());

        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("write.format.default", "PARQUET");
        tableProps.put("write.parquet.compression-codec", "zstd");
        tableProps.put("write.target-file-size-bytes", "134217728");
        tableProps.put("format-version", "2");
        tableProps.put("write.delete.mode", "merge-on-read");
        tableProps.put("write.metadata.delete-after-commit.enabled", "true");
        tableProps.put("write.metadata.previous-versions-max", "5");

        return catalog.createTable(tableId, icebergSchema, spec, tableProps);
    }

    private CatalogLoader buildCatalogLoader(IcebergSinkConfig icebergCfg) {
        Map<String, String> props = buildCatalogProperties(icebergCfg);
        String catalogName = appConfig.getProp("iceberg.catalog.name", "prysm_catalog");

        CatalogType catalogType = icebergCfg.getCatalogType() != null
                ? icebergCfg.getCatalogType() : CatalogType.GLUE;

        switch (catalogType) {
            case GLUE:
                return CatalogLoader.custom(
                        catalogName,
                        props,
                        new org.apache.hadoop.conf.Configuration(),
                        "org.apache.iceberg.aws.glue.GlueCatalog");

            case NESSIE:
                return CatalogLoader.custom(
                        catalogName,
                        props,
                        new org.apache.hadoop.conf.Configuration(),
                        "org.apache.iceberg.nessie.NessieCatalog");

            case HIVE:
                return CatalogLoader.hive(
                        catalogName,
                        new org.apache.hadoop.conf.Configuration(),
                        props);

            default:
                throw new IllegalArgumentException("Unknown catalog type: " + catalogType);
        }
    }

    private Map<String, String> buildCatalogProperties(IcebergSinkConfig icebergCfg) {
        Map<String, String> props = new HashMap<>();

        CatalogType catalogType = icebergCfg.getCatalogType() != null
                ? icebergCfg.getCatalogType() : CatalogType.GLUE;

        switch (catalogType) {
            case GLUE:
                props.put(CatalogProperties.CATALOG_IMPL,
                        "org.apache.iceberg.aws.glue.GlueCatalog");
                props.put(CatalogProperties.WAREHOUSE_LOCATION,
                        appConfig.getIcebergWarehouseUri());
                props.put("glue.region", appConfig.getRegion());
                props.put(CatalogProperties.FILE_IO_IMPL,
                        "org.apache.iceberg.aws.s3.S3FileIO");
                props.put("s3.region", appConfig.getRegion());
                props.put("s3.sse.type", "aws_kms");
                props.put("s3.sse.key", appConfig.getPiiKmsKeyArn());
                break;

            case NESSIE:
                String nessieEndpoint = icebergCfg.getNessieEndpoint() != null
                        ? icebergCfg.getNessieEndpoint()
                        : appConfig.getProp("nessie.endpoint",
                                            "http://nessie:19120/api/v1");
                props.put(CatalogProperties.CATALOG_IMPL,
                        "org.apache.iceberg.nessie.NessieCatalog");
                props.put(CatalogProperties.WAREHOUSE_LOCATION,
                        appConfig.getIcebergWarehouseUri());
                props.put("uri", nessieEndpoint);
                props.put("ref", icebergCfg.getTargetBranch() != null
                        ? icebergCfg.getTargetBranch() : "main");
                props.put(CatalogProperties.FILE_IO_IMPL,
                        "org.apache.iceberg.aws.s3.S3FileIO");
                props.put("s3.region", appConfig.getRegion());
                break;

            case HIVE:
                props.put(CatalogProperties.WAREHOUSE_LOCATION,
                        appConfig.getIcebergWarehouseUri());
                break;
        }
        return props;
    }

    private Catalog buildCatalog(IcebergSinkConfig icebergCfg,
                                  Map<String, String> props) {
        CatalogType catalogType = icebergCfg.getCatalogType() != null
                ? icebergCfg.getCatalogType() : CatalogType.GLUE;
        String catalogName = appConfig.getProp("iceberg.catalog.name", "prysm_catalog");

        switch (catalogType) {
            case GLUE: {
                org.apache.iceberg.aws.glue.GlueCatalog catalog =
                        new org.apache.iceberg.aws.glue.GlueCatalog();
                catalog.initialize(catalogName, props);
                return catalog;
            }
            case NESSIE: {
                // NessieCatalog requires iceberg-nessie on classpath
                try {
                    Class<?> cls = Class.forName("org.apache.iceberg.nessie.NessieCatalog");
                    Catalog catalog = (Catalog) cls.getDeclaredConstructor().newInstance();
                    cls.getMethod("initialize", String.class, Map.class)
                       .invoke(catalog, catalogName, props);
                    return catalog;
                } catch (Exception e) {
                    throw new RuntimeException(
                            "NessieCatalog not available — add iceberg-nessie dependency", e);
                }
            }
            default: {
                org.apache.iceberg.aws.glue.GlueCatalog catalog =
                        new org.apache.iceberg.aws.glue.GlueCatalog();
                catalog.initialize(catalogName, props);
                return catalog;
            }
        }
    }

    private PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionColumns) {
        if (partitionColumns == null || partitionColumns.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        for (String col : partitionColumns) {
            if ("eventDate".equals(col)) {
                specBuilder.day("eventTime", "eventDate");
            } else if (schema.findField(col) != null) {
                specBuilder.identity(col);
            } else {
                LOG.warn("Partition column '{}' not found in Iceberg schema, skipping", col);
            }
        }
        return specBuilder.build();
    }

    private TableIdentifier resolveTableId(IcebergSinkConfig icebergCfg) {
        String dbName = appConfig.getProp("iceberg.glue.database.prefix", "prysm_")
                + icebergCfg.getDatabase() + "_" + appConfig.getEnv();
        return TableIdentifier.of(dbName, icebergCfg.getTable());
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
