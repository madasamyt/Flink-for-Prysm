package com.nuskin.prysm.flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * Central configuration holder for the Prysm Flink pipeline.
 *
 * <h3>Configuration layering</h3>
 * <ol>
 *   <li>{@code application.properties} — structural / non-sensitive defaults,
 *       committed to source control.</li>
 *   <li>Environment variables — override any property key at deployment time
 *       (e.g. {@code PRYSM_ENV}, {@code AWS_REGION}).</li>
 *   <li>AWS SSM Parameter Store — infrastructure config that varies per
 *       environment and must not live in source control (bucket names, stream
 *       names, KMS key ARNs). Resolved via {@link ParameterStoreUtil}.</li>
 *   <li>AWS Secrets Manager — credentials and salts. Resolved via
 *       {@link SecretsManagerUtil}.</li>
 * </ol>
 *
 * <p>Callers access fully-resolved values through typed getter methods.
 * The underlying {@link Properties} object is not exposed publicly to prevent
 * accidental logging of sensitive values.
 */
public class AppConfig implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AppConfig.class);

    private final Properties props = new Properties();
    private final String env;
    private final String region;

    // Lazily-initialised (not serialised — re-created on each task manager)
    private transient ParameterStoreUtil ssmUtil;
    private transient SecretsManagerUtil secretsUtil;

    // ── Resolved values (populated during init) ──────────────────────────────
    private String dataLakeBucket;
    private String checkpointBucket;
    private String piiKmsKeyArn;
    private String piiHmacSecret;

    // ── Factory ──────────────────────────────────────────────────────────────

    public static AppConfig load() {
        AppConfig config = new AppConfig();
        config.loadProperties();
        config.resolveFromAWS();
        return config;
    }

    private AppConfig() {
        // Use environment variables first, fall back to defaults
        this.env    = envOrDefault("PRYSM_ENV", "dev");
        this.region = envOrDefault("AWS_REGION", "us-east-1");
    }

    // ── Initialisation ───────────────────────────────────────────────────────

    private void loadProperties() {
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is == null) {
                throw new IllegalStateException("application.properties not found on classpath");
            }
            props.load(is);
            LOG.info("Loaded application.properties ({} keys)", props.size());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }

    private void resolveFromAWS() {
        if ("local".equals(env)) {
            LOG.warn("Running in LOCAL mode — skipping AWS SSM/Secrets Manager lookups");
            dataLakeBucket    = props.getProperty("iceberg.warehouse.bucket", "local-data-lake");
            checkpointBucket  = props.getProperty("checkpoint.s3.bucket",    "local-checkpoints");
            piiKmsKeyArn      = "LOCAL_KMS_KEY";
            piiHmacSecret     = "local-dev-hmac-secret-change-in-prod";
            return;
        }

        ParameterStoreUtil ssm = getSsmUtil();
        SecretsManagerUtil secrets = getSecretsUtil();

        try {
            dataLakeBucket   = ssm.getParameter("/prysm/" + env + "/config/data-lake-bucket");
            checkpointBucket = ssm.getParameter("/prysm/" + env + "/config/checkpoint-bucket");
            piiKmsKeyArn     = ssm.getSecureParameter("/prysm/" + env + "/config/pii-kms-key-arn");
            piiHmacSecret    = secrets.getSecretField(
                                   "/prysm/" + env + "/secrets/pii", "hmacSecret");

            LOG.info("Resolved AWS config: dataLakeBucket={}, checkpointBucket={}",
                     dataLakeBucket, checkpointBucket);
        } finally {
            ssm.close();
            secrets.close();
        }
    }

    // ── Stream-topology resolution ────────────────────────────────────────────

    /**
     * Loads and resolves the full stream topology, substituting SSM-held
     * stream names into each {@link StreamTopologyConfig.StreamDefinition}.
     */
    public StreamTopologyConfig resolveStreamTopology() {
        StreamTopologyConfig topology = StreamTopologyConfig.load();
        if ("local".equals(env)) {
            // In local/test mode, use predictable stream names
            topology.getStreams().forEach(s ->
                    s.setResolvedStreamName(s.getKey()));
            return topology;
        }

        ParameterStoreUtil ssm = getSsmUtil();
        topology.getStreams().forEach(stream -> {
            String paramPath = ssm.resolvePath(stream.getStreamNameParamPath());
            String streamName = ssm.getParameter(paramPath);
            stream.setResolvedStreamName(streamName);
            LOG.info("Resolved stream {}: {}", stream.getKey(), streamName);
        });
        ssm.close();
        return topology;
    }

    // ── Typed accessors ──────────────────────────────────────────────────────

    public String getEnv()             { return env; }
    public String getRegion()          { return region; }
    public String getDataLakeBucket()  { return dataLakeBucket; }
    public String getCheckpointBucket(){ return checkpointBucket; }
    public String getPiiKmsKeyArn()    { return piiKmsKeyArn; }
    public String getPiiHmacSecret()   { return piiHmacSecret; }
    public boolean isPiiEncryptionEnabled() {
        return Boolean.parseBoolean(props.getProperty("pii.encryption.enabled", "true"));
    }

    public String getIcebergWarehouseUri() {
        String prefix = props.getProperty("iceberg.warehouse.prefix", "iceberg-warehouse");
        return "s3://" + dataLakeBucket + "/" + prefix;
    }

    public String getCheckpointUri() {
        String prefix = props.getProperty("checkpoint.s3.prefix", "flink-checkpoints");
        return "s3://" + checkpointBucket + "/" + prefix;
    }

    public long getCheckpointIntervalMs() {
        return Long.parseLong(props.getProperty("checkpoint.interval.ms", "60000"));
    }

    public int getDefaultParallelism() {
        return Integer.parseInt(props.getProperty("flink.parallelism.default", "4"));
    }

    public int getIngestionParallelism() {
        return Integer.parseInt(props.getProperty("flink.parallelism.ingestion", "8"));
    }

    public String getGlueRegistryRegion() {
        return props.getProperty("glue.schema.registry.region", region);
    }

    public boolean isGlueAutoRegister() {
        return Boolean.parseBoolean(props.getProperty("glue.schema.registry.auto.register", "false"));
    }

    public int getRecommendationProductsPerRec() {
        return Integer.parseInt(props.getProperty("recommendation.products.per.recommendation", "5"));
    }

    public long getIcebergFlushIntervalMs() {
        return Long.parseLong(props.getProperty("iceberg.sink.flush.interval.ms", "30000"));
    }

    public String getProp(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ParameterStoreUtil getSsmUtil() {
        if (ssmUtil == null) {
            ssmUtil = new ParameterStoreUtil(region, env);
        }
        return ssmUtil;
    }

    private SecretsManagerUtil getSecretsUtil() {
        if (secretsUtil == null) {
            secretsUtil = new SecretsManagerUtil(region);
        }
        return secretsUtil;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    @Override
    public String toString() {
        // Deliberately excludes sensitive fields
        return "AppConfig{env=" + env + ", region=" + region
                + ", dataLakeBucket=" + dataLakeBucket
                + ", checkpointBucket=" + checkpointBucket + "}";
    }
}
