package com.nuskin.prysm.flink.schema;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds AWS Glue Schema Registry configuration maps used by both the
 * Kinesis deserialiser and the Iceberg Avro writer.
 *
 * <h3>Schema drift strategy</h3>
 * <p>All schemas are registered with {@code BACKWARD} compatibility, meaning:
 * <ul>
 *   <li>New <em>optional</em> fields (with defaults) can be added freely.</li>
 *   <li>Existing fields cannot be removed or have their types changed.</li>
 *   <li>The {@code metadata} map field in every schema provides an
 *       escape hatch for truly ad-hoc additions without a schema bump.</li>
 * </ul>
 *
 * <p>In production, {@code glue.schema.registry.auto.register} is {@code false}
 * so schema changes go through a deliberate review process. In dev/staging it
 * can be {@code true} to allow rapid iteration.
 */
public class GlueSchemaRegistryConfig {

    private GlueSchemaRegistryConfig() {}

    /**
     * Returns a configuration map suitable for constructing a
     * {@link com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema}.
     */
    public static Map<String, Object> forStream(AppConfig appConfig, StreamDefinition stream) {
        Map<String, Object> config = baseConfig(appConfig);

        String registryName = stream.getGlueRegistryName()
                .replace("{env}", appConfig.getEnv());
        String schemaName   = stream.getGlueSchemaName();

        config.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);
        config.put(AWSSchemaRegistryConstants.SCHEMA_NAME,   schemaName);
        config.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                   AvroRecordType.GENERIC_RECORD.getName());

        return config;
    }

    /**
     * Returns a configuration map suitable for constructing a
     * {@link com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroSerializationSchema}
     * when writing recommendations back to Kinesis.
     */
    public static Map<String, Object> forSerializer(AppConfig appConfig, StreamDefinition stream) {
        Map<String, Object> config = baseConfig(appConfig);

        String registryName = stream.getGlueRegistryName()
                .replace("{env}", appConfig.getEnv());

        config.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);
        config.put(AWSSchemaRegistryConstants.SCHEMA_NAME,   stream.getGlueSchemaName());
        config.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                   appConfig.isGlueAutoRegister());
        config.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING,
                   GlueSchemaRegistryConfiguration.Compatibility.BACKWARD);
        config.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                   AvroRecordType.GENERIC_RECORD.getName());

        return config;
    }

    // ── Internal ─────────────────────────────────────────────────────────────

    private static Map<String, Object> baseConfig(AppConfig appConfig) {
        Map<String, Object> config = new HashMap<>();
        config.put(AWSSchemaRegistryConstants.AWS_REGION, appConfig.getGlueRegistryRegion());
        config.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                   AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString());
        // Schema cache TTL — avoids hitting Glue on every record
        config.put(AWSSchemaRegistryConstants.CACHE_SIZE, 200);
        config.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, 86_400_000L); // 24 h
        return config;
    }
}
