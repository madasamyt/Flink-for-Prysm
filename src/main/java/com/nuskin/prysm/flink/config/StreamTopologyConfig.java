package com.nuskin.prysm.flink.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Full metadata-driven stream topology loaded from {@code streams-topology.yml}.
 *
 * <p>Every aspect of a stream's lifecycle — connector configuration, schema,
 * data quality / prep rules, and sink targets — is declared here.  The Flink
 * engine reads this config once at job startup and interprets it; adding a new
 * stream or changing its quality rules never requires a code change.
 *
 * <h3>Structure</h3>
 * <pre>
 *   pipeline:          (optional global overrides)
 *   streams:
 *     - key: customer-profile
 *       streamType: CUSTOMER
 *       source: { type: KINESIS, ... }
 *       schema: { avroFile: ..., glueSchemaName: ... }
 *       quality: { dedupKey: ..., requiredFields: [...], ... }
 *       sinks:
 *         - { type: ICEBERG, ... }
 *         - { type: DREMIO_FLIGHT, ... }   # optional extra sinks
 *   dlq:               (dead-letter queue target)
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamTopologyConfig implements Serializable {

    private PipelineConfig pipeline = new PipelineConfig();
    private List<StreamDefinition> streams = new ArrayList<>();
    private DlqConfig dlq = new DlqConfig();

    // ── Factory ──────────────────────────────────────────────────────────────

    public static StreamTopologyConfig load() {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            InputStream is = StreamTopologyConfig.class
                    .getClassLoader()
                    .getResourceAsStream("streams-topology.yml");
            if (is == null) {
                throw new IllegalStateException("streams-topology.yml not found on classpath");
            }
            return mapper.readValue(is, StreamTopologyConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load stream topology config", e);
        }
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public PipelineConfig getPipeline()            { return pipeline; }
    public void setPipeline(PipelineConfig p)      { this.pipeline = p; }

    public List<StreamDefinition> getStreams()     { return streams; }
    public void setStreams(List<StreamDefinition> s) { this.streams = s; }

    public DlqConfig getDlq()                      { return dlq; }
    public void setDlq(DlqConfig dlq)              { this.dlq = dlq; }

    // =========================================================================
    // Inner: optional global pipeline overrides
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PipelineConfig implements Serializable {
        private String name;
        private long checkpointIntervalMs = -1;  // -1 → use application.properties value
        private int parallelism = -1;             // -1 → use application.properties value

        public String getName()                       { return name; }
        public void setName(String name)              { this.name = name; }
        public long getCheckpointIntervalMs()         { return checkpointIntervalMs; }
        public void setCheckpointIntervalMs(long ms)  { this.checkpointIntervalMs = ms; }
        public int getParallelism()                   { return parallelism; }
        public void setParallelism(int p)             { this.parallelism = p; }
        public boolean hasCheckpointOverride()        { return checkpointIntervalMs > 0; }
        public boolean hasParallelismOverride()       { return parallelism > 0; }
    }

    // =========================================================================
    // Inner: per-stream definition
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamDefinition implements Serializable {

        /** Logical stream key used in metric names and operator UIDs */
        private String key;

        /** High-level domain classification */
        private StreamType streamType = StreamType.UNKNOWN;

        /** Connector configuration — type-discriminated via Jackson polymorphism */
        private SourceConfig source;

        /** Schema registry configuration */
        private SchemaConfig schema = new SchemaConfig();

        /** All data quality and preparation rules for this stream (optional) */
        private QualityConfig quality;

        /** One or more sink targets; at least one ICEBERG sink is expected */
        private List<SinkConfig> sinks = new ArrayList<>();

        // ── Getters / setters ─────────────────────────────────────────────────

        public String getKey()                        { return key; }
        public void setKey(String key)                { this.key = key; }

        public StreamType getStreamType()             { return streamType; }
        public void setStreamType(StreamType t)       { this.streamType = t; }

        public SourceConfig getSource()               { return source; }
        public void setSource(SourceConfig source)    { this.source = source; }

        public SchemaConfig getSchema()               { return schema; }
        public void setSchema(SchemaConfig schema)    { this.schema = schema; }

        public QualityConfig getQuality()             { return quality; }
        public void setQuality(QualityConfig quality) { this.quality = quality; }

        public List<SinkConfig> getSinks()            { return sinks; }
        public void setSinks(List<SinkConfig> sinks)  { this.sinks = sinks; }

        // ── Convenience helpers ───────────────────────────────────────────────

        /** Returns true when this stream carries health/biometric scan data */
        public boolean isScanStream() { return StreamType.SCAN == streamType; }

        /** Returns true when the quality config declares PII sensitive fields */
        public boolean hasSensitiveFields() { return !getSensitiveFields().isEmpty(); }

        /** Returns the first ICEBERG sink config, or null if none configured */
        public IcebergSinkConfig primaryIcebergSink() {
            if (sinks == null) return null;
            return sinks.stream()
                    .filter(s -> SinkType.ICEBERG == s.getType())
                    .map(s -> (IcebergSinkConfig) s)
                    .findFirst()
                    .orElse(null);
        }

        // ── Backward-compatible delegate getters ──────────────────────────────
        // These allow existing code (KinesisSourceBuilder, IcebergSinkBuilder,
        // PIIEncryptionTransform) to compile without change while the YAML
        // structure is richer.

        public String getResolvedStreamName() {
            if (source instanceof KinesisSourceConfig) {
                return ((KinesisSourceConfig) source).getResolvedStreamName();
            }
            return null;
        }

        public void setResolvedStreamName(String name) {
            if (source instanceof KinesisSourceConfig) {
                ((KinesisSourceConfig) source).setResolvedStreamName(name);
            }
        }

        /** SSM path for the Kinesis stream name */
        public String getStreamNameParamPath() {
            if (source instanceof KinesisSourceConfig) {
                return ((KinesisSourceConfig) source).getStreamNameParamPath();
            }
            return null;
        }

        /** EFO consumer name template */
        public String getEfoConsumer() {
            if (source instanceof KinesisSourceConfig) {
                return ((KinesisSourceConfig) source).getEfoConsumer();
            }
            return null;
        }

        /** Avro schema filename (relative to {@code avro/} on classpath) */
        public String getAvroSchema() {
            return schema != null ? schema.getAvroFile() : null;
        }

        public String getGlueRegistryName() {
            return schema != null ? schema.getGlueRegistryName() : null;
        }

        public String getGlueSchemaName() {
            return schema != null ? schema.getGlueSchemaName() : null;
        }

        /** Qualified {@code db.table} for the primary Iceberg sink */
        public String getIcebergTable() {
            IcebergSinkConfig iceberg = primaryIcebergSink();
            if (iceberg == null) return null;
            return iceberg.getDatabase() + "." + iceberg.getTable();
        }

        /** Partition columns for the primary Iceberg sink */
        public List<String> getPartitionBy() {
            IcebergSinkConfig iceberg = primaryIcebergSink();
            if (iceberg == null) return Collections.emptyList();
            return iceberg.getPartitionColumns() != null
                    ? iceberg.getPartitionColumns() : Collections.emptyList();
        }

        /** Fields requiring PII encryption — sourced from quality config */
        public List<String> getSensitiveFields() {
            if (quality == null || quality.getSensitiveFields() == null) {
                return Collections.emptyList();
            }
            return quality.getSensitiveFields();
        }

        /** Watermark out-of-orderness lag; defaults to 30 s */
        public int getWatermarkLagSeconds() {
            if (source instanceof KinesisSourceConfig) {
                return ((KinesisSourceConfig) source).getWatermarkLagSeconds();
            }
            return 30;
        }

        @Override
        public String toString() {
            return "StreamDefinition{key='" + key + "', type=" + streamType + "}";
        }
    }

    // =========================================================================
    // Inner: SOURCE CONFIG — polymorphic on "type" field
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(
            use       = JsonTypeInfo.Id.NAME,
            include   = JsonTypeInfo.As.EXISTING_PROPERTY,
            property  = "type",
            visible   = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = KinesisSourceConfig.class, name = "KINESIS"),
            @JsonSubTypes.Type(value = KafkaSourceConfig.class,   name = "KAFKA"),
    })
    public abstract static class SourceConfig implements Serializable {
        protected SourceType type;
        public SourceType getType()            { return type; }
        public void setType(SourceType type)   { this.type = type; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KinesisSourceConfig extends SourceConfig implements Serializable {
        private String streamNameParamPath;
        private String efoConsumer;
        private int watermarkLagSeconds   = 30;
        private int idleTimeoutSeconds    = 300;

        /** Resolved at runtime from SSM; not in YAML */
        private transient String resolvedStreamName;

        public String getStreamNameParamPath()                  { return streamNameParamPath; }
        public void setStreamNameParamPath(String p)            { this.streamNameParamPath = p; }
        public String getEfoConsumer()                          { return efoConsumer; }
        public void setEfoConsumer(String c)                    { this.efoConsumer = c; }
        public int getWatermarkLagSeconds()                     { return watermarkLagSeconds; }
        public void setWatermarkLagSeconds(int s)               { this.watermarkLagSeconds = s; }
        public int getIdleTimeoutSeconds()                      { return idleTimeoutSeconds; }
        public void setIdleTimeoutSeconds(int s)                { this.idleTimeoutSeconds = s; }
        public String getResolvedStreamName()                   { return resolvedStreamName; }
        public void setResolvedStreamName(String name)          { this.resolvedStreamName = name; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KafkaSourceConfig extends SourceConfig implements Serializable {
        private String bootstrapServers;
        private String topic;
        private String consumerGroup;
        private int watermarkLagSeconds = 10;

        public String getBootstrapServers()          { return bootstrapServers; }
        public void setBootstrapServers(String s)    { this.bootstrapServers = s; }
        public String getTopic()                     { return topic; }
        public void setTopic(String t)               { this.topic = t; }
        public String getConsumerGroup()             { return consumerGroup; }
        public void setConsumerGroup(String g)       { this.consumerGroup = g; }
        public int getWatermarkLagSeconds()          { return watermarkLagSeconds; }
        public void setWatermarkLagSeconds(int s)    { this.watermarkLagSeconds = s; }
    }

    // =========================================================================
    // Inner: SCHEMA CONFIG
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SchemaConfig implements Serializable {
        private String avroFile;
        private String glueSchemaName;
        private String glueRegistryName;

        public String getAvroFile()                  { return avroFile; }
        public void setAvroFile(String f)            { this.avroFile = f; }
        public String getGlueSchemaName()            { return glueSchemaName; }
        public void setGlueSchemaName(String n)      { this.glueSchemaName = n; }
        public String getGlueRegistryName()          { return glueRegistryName; }
        public void setGlueRegistryName(String n)    { this.glueRegistryName = n; }
    }

    // =========================================================================
    // Inner: QUALITY CONFIG — fully metadata-driven data prep rules
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QualityConfig implements Serializable {

        /** Field used as the deduplication key; null = deduplication disabled */
        private String dedupKey;

        /** How long to keep dedup state; defaults to 24 hours */
        private int dedupTtlHours = 24;

        /** Fields that must be non-null/non-blank; missing → DLQ */
        private List<String> requiredFields = new ArrayList<>();

        /** What to do when non-required fields are null */
        private NullStrategy nullStrategy = NullStrategy.FORWARD;

        /** Default values to fill in when nullStrategy = FILL_DEFAULT */
        private Map<String, String> fieldDefaults = new HashMap<>();

        /** Type coercions applied before validation */
        private List<TypeCoercion> typeCoercions = new ArrayList<>();

        /** Per-field validation rules */
        private List<FieldValidation> fieldValidations = new ArrayList<>();

        /** Anomaly detection rules applied after all other quality steps */
        private List<AnomalyRule> anomalyDetection = new ArrayList<>();

        /** Fields requiring PII encryption via KMS */
        private List<String> sensitiveFields = new ArrayList<>();

        // Getters / setters
        public String getDedupKey()                          { return dedupKey; }
        public void setDedupKey(String k)                    { this.dedupKey = k; }
        public int getDedupTtlHours()                        { return dedupTtlHours; }
        public void setDedupTtlHours(int h)                  { this.dedupTtlHours = h; }
        public List<String> getRequiredFields()              { return requiredFields; }
        public void setRequiredFields(List<String> f)        { this.requiredFields = f; }
        public NullStrategy getNullStrategy()                { return nullStrategy; }
        public void setNullStrategy(NullStrategy s)          { this.nullStrategy = s; }
        public Map<String, String> getFieldDefaults()        { return fieldDefaults; }
        public void setFieldDefaults(Map<String, String> d)  { this.fieldDefaults = d; }
        public List<TypeCoercion> getTypeCoercions()         { return typeCoercions; }
        public void setTypeCoercions(List<TypeCoercion> t)   { this.typeCoercions = t; }
        public List<FieldValidation> getFieldValidations()   { return fieldValidations; }
        public void setFieldValidations(List<FieldValidation> v) { this.fieldValidations = v; }
        public List<AnomalyRule> getAnomalyDetection()       { return anomalyDetection; }
        public void setAnomalyDetection(List<AnomalyRule> a) { this.anomalyDetection = a; }
        public List<String> getSensitiveFields()             { return sensitiveFields; }
        public void setSensitiveFields(List<String> f)       { this.sensitiveFields = f; }

        public boolean hasDedupKey()       { return dedupKey != null && !dedupKey.isBlank(); }
        public boolean hasCoercions()      { return typeCoercions != null && !typeCoercions.isEmpty(); }
        public boolean hasValidations()    { return fieldValidations != null && !fieldValidations.isEmpty(); }
        public boolean hasAnomalyRules()   { return anomalyDetection != null && !anomalyDetection.isEmpty(); }
        public boolean hasRequiredFields() { return requiredFields != null && !requiredFields.isEmpty(); }
    }

    // ── Quality: NullStrategy ──────────────────────────────────────────────────

    public enum NullStrategy {
        /** Drop the entire record and route to DLQ */
        DROP_RECORD,
        /** Fill nulls using fieldDefaults map; if no default, forward as-is */
        FILL_DEFAULT,
        /** Forward record unchanged (default) */
        FORWARD
    }

    // ── Quality: TypeCoercion ─────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TypeCoercion implements Serializable {
        private String field;
        private CoercionType fromType;
        private CoercionType toType;

        public String getField()                { return field; }
        public void setField(String f)          { this.field = f; }
        public CoercionType getFromType()       { return fromType; }
        public void setFromType(CoercionType t) { this.fromType = t; }
        public CoercionType getToType()         { return toType; }
        public void setToType(CoercionType t)   { this.toType = t; }
    }

    public enum CoercionType {
        ISO8601_STRING,
        EPOCH_MILLIS,
        UPPERCASE_ENUM,
        LOWERCASE_STRING,
        TRIM_STRING,
        NUMERIC_STRING
    }

    // ── Quality: FieldValidation ──────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldValidation implements Serializable {
        private String field;
        private ValidationRule rule;
        private String pattern;         // REGEX
        private Double min;             // RANGE
        private Double max;             // RANGE
        private List<String> allowedValues = new ArrayList<>(); // ENUM_VALUE
        private OnFail onFail = OnFail.DLQ;
        private String defaultValue;    // used when onFail = FILL_DEFAULT

        public String getField()                       { return field; }
        public void setField(String f)                 { this.field = f; }
        public ValidationRule getRule()                { return rule; }
        public void setRule(ValidationRule r)          { this.rule = r; }
        public String getPattern()                     { return pattern; }
        public void setPattern(String p)               { this.pattern = p; }
        public Double getMin()                         { return min; }
        public void setMin(Double m)                   { this.min = m; }
        public Double getMax()                         { return max; }
        public void setMax(Double m)                   { this.max = m; }
        public List<String> getAllowedValues()          { return allowedValues; }
        public void setAllowedValues(List<String> v)   { this.allowedValues = v; }
        public OnFail getOnFail()                      { return onFail; }
        public void setOnFail(OnFail o)                { this.onFail = o; }
        public String getDefaultValue()                { return defaultValue; }
        public void setDefaultValue(String d)          { this.defaultValue = d; }
    }

    public enum ValidationRule { REGEX, RANGE, NOT_EMPTY, ENUM_VALUE }

    public enum OnFail { DLQ, FILL_DEFAULT, DROP_RECORD, FORWARD }

    // ── Quality: AnomalyRule ──────────────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AnomalyRule implements Serializable {
        private String field;
        private AnomalyMethod method = AnomalyMethod.STDDEV;
        private int windowMinutes = 60;
        private double threshold  = 3.0;
        private OnAnomaly onAnomaly = OnAnomaly.FLAG_AND_FORWARD;

        public String getField()                  { return field; }
        public void setField(String f)            { this.field = f; }
        public AnomalyMethod getMethod()          { return method; }
        public void setMethod(AnomalyMethod m)    { this.method = m; }
        public int getWindowMinutes()             { return windowMinutes; }
        public void setWindowMinutes(int m)       { this.windowMinutes = m; }
        public double getThreshold()              { return threshold; }
        public void setThreshold(double t)        { this.threshold = t; }
        public OnAnomaly getOnAnomaly()           { return onAnomaly; }
        public void setOnAnomaly(OnAnomaly o)     { this.onAnomaly = o; }
    }

    public enum AnomalyMethod { STDDEV, IQR, THRESHOLD }

    public enum OnAnomaly { FLAG_AND_FORWARD, DLQ, DROP_RECORD }

    // =========================================================================
    // Inner: SINK CONFIG — polymorphic on "type" field
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeInfo(
            use       = JsonTypeInfo.Id.NAME,
            include   = JsonTypeInfo.As.EXISTING_PROPERTY,
            property  = "type",
            visible   = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = IcebergSinkConfig.class,         name = "ICEBERG"),
            @JsonSubTypes.Type(value = DremioFlightSinkConfig.class,    name = "DREMIO_FLIGHT"),
            @JsonSubTypes.Type(value = DatabricksDeltaSinkConfig.class, name = "DATABRICKS_DELTA"),
    })
    public abstract static class SinkConfig implements Serializable {
        protected SinkType type;
        public SinkType getType()            { return type; }
        public void setType(SinkType type)   { this.type = type; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IcebergSinkConfig extends SinkConfig implements Serializable {
        private CatalogType catalogType = CatalogType.GLUE;
        private String database;
        private String table;
        private List<String> partitionColumns = new ArrayList<>();
        private WriteMode writeMode = WriteMode.APPEND;
        private String upsertKey;
        private String nessieEndpoint;
        private String targetBranch = "main";

        public CatalogType getCatalogType()              { return catalogType; }
        public void setCatalogType(CatalogType c)        { this.catalogType = c; }
        public String getDatabase()                      { return database; }
        public void setDatabase(String d)                { this.database = d; }
        public String getTable()                         { return table; }
        public void setTable(String t)                   { this.table = t; }
        public List<String> getPartitionColumns()        { return partitionColumns; }
        public void setPartitionColumns(List<String> c)  { this.partitionColumns = c; }
        public WriteMode getWriteMode()                  { return writeMode; }
        public void setWriteMode(WriteMode m)            { this.writeMode = m; }
        public String getUpsertKey()                     { return upsertKey; }
        public void setUpsertKey(String k)               { this.upsertKey = k; }
        public String getNessieEndpoint()                { return nessieEndpoint; }
        public void setNessieEndpoint(String e)          { this.nessieEndpoint = e; }
        public String getTargetBranch()                  { return targetBranch; }
        public void setTargetBranch(String b)            { this.targetBranch = b; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DremioFlightSinkConfig extends SinkConfig implements Serializable {
        private String endpoint;
        private String secretsManagerKey;
        private String targetTable;
        private int bufferSize      = 1000;
        private long flushIntervalMs = 5000L;

        public String getEndpoint()                  { return endpoint; }
        public void setEndpoint(String e)            { this.endpoint = e; }
        public String getSecretsManagerKey()         { return secretsManagerKey; }
        public void setSecretsManagerKey(String k)   { this.secretsManagerKey = k; }
        public String getTargetTable()               { return targetTable; }
        public void setTargetTable(String t)         { this.targetTable = t; }
        public int getBufferSize()                   { return bufferSize; }
        public void setBufferSize(int b)             { this.bufferSize = b; }
        public long getFlushIntervalMs()             { return flushIntervalMs; }
        public void setFlushIntervalMs(long ms)      { this.flushIntervalMs = ms; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DatabricksDeltaSinkConfig extends SinkConfig implements Serializable {
        private DatabricksMode mode = DatabricksMode.DELTA;
        private String deltaPath;
        private String jdbcUrl;
        private String secretsManagerKey;
        private String targetTable;

        public DatabricksMode getMode()              { return mode; }
        public void setMode(DatabricksMode m)        { this.mode = m; }
        public String getDeltaPath()                 { return deltaPath; }
        public void setDeltaPath(String p)           { this.deltaPath = p; }
        public String getJdbcUrl()                   { return jdbcUrl; }
        public void setJdbcUrl(String u)             { this.jdbcUrl = u; }
        public String getSecretsManagerKey()         { return secretsManagerKey; }
        public void setSecretsManagerKey(String k)   { this.secretsManagerKey = k; }
        public String getTargetTable()               { return targetTable; }
        public void setTargetTable(String t)         { this.targetTable = t; }
    }

    // =========================================================================
    // Inner: DLQ CONFIG
    // =========================================================================

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DlqConfig implements Serializable {
        private DlqType type = DlqType.ICEBERG;
        private String catalogType = "GLUE";
        private String database    = "system";
        private String table       = "dlq_events";
        private String kafkaTopic;
        private String s3Path;

        public DlqType getType()             { return type; }
        public void setType(DlqType t)       { this.type = t; }
        public String getCatalogType()       { return catalogType; }
        public void setCatalogType(String c) { this.catalogType = c; }
        public String getDatabase()          { return database; }
        public void setDatabase(String d)    { this.database = d; }
        public String getTable()             { return table; }
        public void setTable(String t)       { this.table = t; }
        public String getKafkaTopic()        { return kafkaTopic; }
        public void setKafkaTopic(String t)  { this.kafkaTopic = t; }
        public String getS3Path()            { return s3Path; }
        public void setS3Path(String p)      { this.s3Path = p; }
    }

    // =========================================================================
    // Enumerations
    // =========================================================================

    public enum StreamType { CUSTOMER, SCAN, PRODUCT, COMMERCE, DEVICE, UNKNOWN }

    public enum SourceType { KINESIS, KAFKA, RABBITMQ, SQS, CDC, FILE }

    public enum SinkType { ICEBERG, DREMIO_FLIGHT, DATABRICKS_DELTA, REDSHIFT, ELASTICSEARCH }

    public enum CatalogType { GLUE, HIVE, NESSIE }

    public enum WriteMode { APPEND, UPSERT }

    public enum DatabricksMode { DELTA, JDBC }

    public enum DlqType { ICEBERG, KAFKA, S3 }
}
