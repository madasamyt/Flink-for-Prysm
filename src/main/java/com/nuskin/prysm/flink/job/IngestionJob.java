package com.nuskin.prysm.flink.job;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.sink.DlqSinkBuilder;
import com.nuskin.prysm.flink.sink.SinkConnectorFactory;
import com.nuskin.prysm.flink.source.SourceConnectorFactory;
import com.nuskin.prysm.flink.transform.PIIEncryptionTransform;
import com.nuskin.prysm.flink.transform.quality.QualityChainBuilder;
import com.nuskin.prysm.flink.transform.quality.QualityChainBuilder.QualityChainResult;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h2>Ingestion Job — metadata-driven multi-source, multi-sink pipeline</h2>
 *
 * <p>This job is fully driven by {@code streams-topology.yml}. Adding a new
 * stream, changing its quality rules, or adding a new sink target requires
 * only a YAML edit — no code changes.
 *
 * <h3>Per-stream pipeline (assembled from YAML metadata at startup)</h3>
 * <pre>
 *   SourceConnectorFactory          (source.type: KINESIS / KAFKA / CDC / FILE)
 *        │
 *        ▼
 *   QualityChainBuilder             (quality: requiredFields, typeCoercions,
 *        │  clean stream            nullStrategy, fieldValidations, dedup, anomaly)
 *        │  DLQ side-output ──────────────────────────────────────────────┐
 *        ▼                                                                 │
 *   PIIEncryptionTransform          (quality.sensitiveFields)             │
 *        │                                                                 │
 *        ▼                                                                 │
 *   SinkConnectorFactory            (sinks[]: ICEBERG / DREMIO_FLIGHT /  │
 *        │                           DATABRICKS_DELTA / ...)              │
 *        ▼                                                                 │
 *   [Iceberg, Dremio, Databricks…]                   DlqSinkBuilder ◄────┘
 * </pre>
 *
 * <h3>Fault tolerance</h3>
 * <p>RocksDB incremental checkpoints to S3 every 60 s (configurable via
 * {@code pipeline.checkpointIntervalMs} in the YAML or
 * {@code checkpoint.interval.ms} in {@code application.properties}).
 * Exactly-once semantics end-to-end via Kinesis EFO + Iceberg atomic commits.
 */
public class IngestionJob {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionJob.class);

    public static void main(String[] args) throws Exception {
        new IngestionJob().run();
    }

    public void run() throws Exception {
        // ── 1. Load config and resolve all runtime-dynamic values ────────────
        AppConfig appConfig = AppConfig.load();
        StreamTopologyConfig topology = appConfig.resolveStreamTopology();

        LOG.info("Starting IngestionJob — env={}, streams={}",
                 appConfig.getEnv(), topology.getStreams().size());

        // ── 2. Build Flink execution environment ─────────────────────────────
        StreamExecutionEnvironment env = buildEnv(appConfig, topology);

        // ── 3. Build source factory and sink factory ──────────────────────────
        SourceConnectorFactory sourceFactory =
                new SourceConnectorFactory(appConfig, topology);
        SinkConnectorFactory sinkFactory =
                new SinkConnectorFactory(appConfig);

        // ── 4. Per-stream pipeline assembly ───────────────────────────────────
        for (StreamDefinition streamDef : topology.getStreams()) {

            // 4a. Source — type driven by source.type in YAML
            DataStream<RawStreamEvent> raw = sourceFactory
                    .buildSingleSource(streamDef, env);

            // 4b. Quality chain — all rules driven by quality{} block in YAML
            //     Returns a clean stream and a DLQ side-output stream
            QualityChainResult quality = QualityChainBuilder.attach(raw, streamDef);

            // 4c. PII encryption — sensitive fields from quality.sensitiveFields in YAML
            DataStream<RawStreamEvent> encrypted = quality.clean()
                    .map(new PIIEncryptionTransform(appConfig))
                    .name("pii-encrypt-" + streamDef.getKey())
                    .uid("pii-encrypt-" + streamDef.getKey())
                    .setParallelism(appConfig.getIngestionParallelism());

            // 4d. Sinks — all targets driven by sinks[] block in YAML
            sinkFactory.attachAll(encrypted, streamDef);

            // 4e. DLQ — route failed events to configured DLQ target
            DlqSinkBuilder.attach(quality.dlq(), topology.getDlq(), appConfig);

            LOG.info("Wired pipeline for stream '{}'", streamDef.getKey());
        }

        // ── 5. Execute ────────────────────────────────────────────────────────
        String pipelineName = topology.getPipeline().getName() != null
                ? topology.getPipeline().getName()
                : appConfig.getProp("flink.job.name", "prysm-ingestion");
        String jobName = pipelineName + "-" + appConfig.getEnv();
        LOG.info("Submitting job: {}", jobName);
        env.execute(jobName);
    }

    // ── Environment setup ─────────────────────────────────────────────────────

    private StreamExecutionEnvironment buildEnv(AppConfig appConfig,
                                                 StreamTopologyConfig topology) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism — pipeline YAML overrides application.properties
        int parallelism = topology.getPipeline().hasParallelismOverride()
                ? topology.getPipeline().getParallelism()
                : appConfig.getDefaultParallelism();
        env.setParallelism(parallelism);

        // RocksDB state backend with incremental checkpoints to S3
        EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDB);

        // Checkpointing — pipeline YAML overrides application.properties
        long checkpointIntervalMs = topology.getPipeline().hasCheckpointOverride()
                ? topology.getPipeline().getCheckpointIntervalMs()
                : appConfig.getCheckpointIntervalMs();
        env.enableCheckpointing(checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointTimeout(300_000L);
        cc.setMinPauseBetweenCheckpoints(5_000L);
        cc.setMaxConcurrentCheckpoints(1);
        cc.setCheckpointStorage(appConfig.getCheckpointUri());
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
