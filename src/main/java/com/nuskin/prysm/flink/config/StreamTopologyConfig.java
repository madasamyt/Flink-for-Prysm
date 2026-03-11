package com.nuskin.prysm.flink.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the full stream topology loaded from {@code streams-topology.yml}.
 * Each entry maps one Kinesis stream to its Avro schema, Glue registry entry,
 * Iceberg table, PII field list, and EFO consumer name.
 *
 * <p>Stream ARNs and bucket names are intentionally absent here — they are
 * resolved at job startup from AWS SSM Parameter Store via
 * {@link ParameterStoreUtil} using the {@code streamNameParamPath} template.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamTopologyConfig implements Serializable {

    private List<StreamDefinition> streams = new ArrayList<>();

    // ── Factory ─────────────────────────────────────────────────────────────

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

    // ── Accessors ────────────────────────────────────────────────────────────

    public List<StreamDefinition> getStreams() { return streams; }
    public void setStreams(List<StreamDefinition> streams) { this.streams = streams; }

    // ── Inner: per-stream definition ─────────────────────────────────────────

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamDefinition implements Serializable {

        /** Short logical key — used in metric names and SSM path substitution */
        private String key;

        /** SSM Parameter Store path template for the Kinesis stream name.
         *  The placeholder {@code {env}} is substituted at startup. */
        private String streamNameParamPath;

        /** Avro schema resource file name (relative to avro/ on classpath) */
        private String avroSchema;

        /** AWS Glue Schema Registry name (with {env} placeholder) */
        private String glueRegistryName;

        /** Schema name inside the Glue registry */
        private String glueSchemaName;

        /** Fully-qualified Iceberg table name: {@code <db>.<table>} */
        private String icebergTable;

        /** Iceberg partition columns */
        private List<String> partitionBy = new ArrayList<>();

        /**
         * Field names that contain PII / sensitive data.
         * These fields will be encrypted with KMS before writing to Iceberg
         * and pseudonymised with HMAC-SHA256 in derived analytics tables.
         */
        private List<String> sensitiveFields = new ArrayList<>();

        /** High-level domain grouping for fan-out routing */
        private StreamType streamType = StreamType.UNKNOWN;

        /** EFO consumer name (with {env} placeholder) */
        private String efoConsumer;

        // ── Resolved at runtime (not in YAML) ────────────────────────────────

        /** Resolved Kinesis stream name after SSM lookup */
        private transient String resolvedStreamName;

        // ── Getters / setters ─────────────────────────────────────────────────

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getStreamNameParamPath() { return streamNameParamPath; }
        public void setStreamNameParamPath(String path) { this.streamNameParamPath = path; }

        public String getAvroSchema() { return avroSchema; }
        public void setAvroSchema(String avroSchema) { this.avroSchema = avroSchema; }

        public String getGlueRegistryName() { return glueRegistryName; }
        public void setGlueRegistryName(String name) { this.glueRegistryName = name; }

        public String getGlueSchemaName() { return glueSchemaName; }
        public void setGlueSchemaName(String name) { this.glueSchemaName = name; }

        public String getIcebergTable() { return icebergTable; }
        public void setIcebergTable(String table) { this.icebergTable = table; }

        public List<String> getPartitionBy() { return partitionBy; }
        public void setPartitionBy(List<String> partitionBy) { this.partitionBy = partitionBy; }

        public List<String> getSensitiveFields() { return sensitiveFields; }
        public void setSensitiveFields(List<String> fields) { this.sensitiveFields = fields; }

        public StreamType getStreamType() { return streamType; }
        public void setStreamType(StreamType streamType) { this.streamType = streamType; }

        public String getEfoConsumer() { return efoConsumer; }
        public void setEfoConsumer(String efoConsumer) { this.efoConsumer = efoConsumer; }

        public String getResolvedStreamName() { return resolvedStreamName; }
        public void setResolvedStreamName(String name) { this.resolvedStreamName = name; }

        /** Returns true when this stream carries health/biometric scan data */
        public boolean isScanStream() { return StreamType.SCAN == streamType; }

        /** Returns true when this stream carries PII that must be encrypted */
        public boolean hasSensitiveFields() { return !sensitiveFields.isEmpty(); }

        @Override
        public String toString() {
            return "StreamDefinition{key='" + key + "', table='" + icebergTable + "'}";
        }
    }

    // ── Inner: stream domain type ─────────────────────────────────────────────

    public enum StreamType {
        CUSTOMER, SCAN, PRODUCT, COMMERCE, DEVICE, UNKNOWN
    }
}
