package com.nuskin.prysm.flink.job;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.sink.IcebergSinkBuilder;
import com.nuskin.prysm.flink.source.KinesisSourceBuilder;
import com.nuskin.prysm.flink.transform.PIIEncryptionTransform;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h2>Ingestion Job</h2>
 *
 * Reads all 19 Kinesis streams → applies PII encryption → routes each event
 * to its dedicated Apache Iceberg table in the S3 data lake.
 *
 * <pre>
 *   Kinesis stream 1 ──┐
 *   Kinesis stream 2 ──┤
 *   ...                ├──► union ──► PII encrypt ──► fan-out ──► Iceberg table per stream
 *   Kinesis stream 19 ─┘
 * </pre>
 *
 * <h3>Fault tolerance</h3>
 * <p>Checkpointing to S3 every 60 s. RocksDB incremental checkpoints keep
 * checkpoint sizes small. The job uses exactly-once semantics end-to-end:
 * Kinesis EFO (exactly-once at source) and Iceberg atomic commits (exactly-once
 * at sink).
 *
 * <h3>Running</h3>
 * <pre>
 *   flink run -c com.nuskin.prysm.flink.job.IngestionJob \
 *       target/flink-prysm-pipeline-1.0.0-fat.jar \
 *       --job ingestion
 * </pre>
 */
public class IngestionJob {

    private static final Logger LOG = LoggerFactory.getLogger(IngestionJob.class);

    public static void main(String[] args) throws Exception {
        new IngestionJob().run();
    }

    public void run() throws Exception {
        // ── 1. Load config and resolve all SSM / Secrets Manager values ──────
        AppConfig appConfig = AppConfig.load();
        StreamTopologyConfig topology = appConfig.resolveStreamTopology();
        LOG.info("Starting IngestionJob — env={}, streams={}",
                 appConfig.getEnv(), topology.getStreams().size());

        // ── 2. Build Flink execution environment ─────────────────────────────
        StreamExecutionEnvironment env = buildEnv(appConfig);

        // ── 3. Build union source (all 19 streams) ───────────────────────────
        KinesisSourceBuilder sourceBuilder = new KinesisSourceBuilder(appConfig, topology);
        DataStream<RawStreamEvent> rawStream = sourceBuilder.buildUnionSource(env);

        // ── 4. PII encryption (AES-256-GCM envelope encryption via KMS) ──────
        DataStream<RawStreamEvent> encryptedStream = rawStream
                .map(new PIIEncryptionTransform(appConfig))
                .name("PII-Encrypt")
                .uid("pii-encrypt")
                .setParallelism(appConfig.getIngestionParallelism());

        // ── 5. Fan-out: one Iceberg sink per stream ───────────────────────────
        IcebergSinkBuilder sinkBuilder = new IcebergSinkBuilder(appConfig);

        for (StreamDefinition streamDef : topology.getStreams()) {
            // Filter to events for this stream only
            DataStream<RawStreamEvent> streamEvents = encryptedStream
                    .filter(event -> streamDef.getKey().equals(event.getStreamKey()))
                    .name("filter-" + streamDef.getKey())
                    .uid("filter-" + streamDef.getKey());

            sinkBuilder.attachSink(streamEvents, streamDef);
            LOG.info("Wired sink for stream: {}", streamDef.getKey());
        }

        // ── 6. Execute ────────────────────────────────────────────────────────
        String jobName = appConfig.getProp("flink.job.name", "prysm-ingestion")
                + "-" + appConfig.getEnv();
        LOG.info("Submitting job: {}", jobName);
        env.execute(jobName);
    }

    // ── Environment setup ────────────────────────────────────────────────────

    private StreamExecutionEnvironment buildEnv(AppConfig appConfig) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Parallelism
        env.setParallelism(appConfig.getDefaultParallelism());

        // RocksDB state backend with incremental checkpoints to S3
        EmbeddedRocksDBStateBackend rocksDB = new EmbeddedRocksDBStateBackend(true);
        env.setStateBackend(rocksDB);

        // Checkpointing
        env.enableCheckpointing(appConfig.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointTimeout(300_000L);
        cc.setMinPauseBetweenCheckpoints(5_000L);
        cc.setMaxConcurrentCheckpoints(1);
        cc.setCheckpointStorage(appConfig.getCheckpointUri());
        // Retain checkpoints on job cancellation so the job can resume from the last checkpoint
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;
    }
}
