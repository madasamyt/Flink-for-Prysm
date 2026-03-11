package com.nuskin.prysm.flink.job;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.model.SkinHealthSnapshot;
import com.nuskin.prysm.flink.source.KinesisSourceBuilder;
import com.nuskin.prysm.flink.transform.RecommendationEngine;
import com.nuskin.prysm.flink.transform.RecommendationOutput;
import com.nuskin.prysm.flink.transform.ScanMetricsAggregator;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <h2>Recommendation Job</h2>
 *
 * Combines scan health snapshots with the live product catalogue to generate
 * personalised product recommendations in real time.
 *
 * <pre>
 *   scan-* streams ──► ScanMetricsAggregator ──► SkinHealthSnapshot
 *                                                        │ (keyed by customerId)
 *   product-catalog ──► broadcast ───────────────────────┤
 *                                                        ▼
 *                                             RecommendationEngine
 *                                                        │
 *                             ┌──────────────────────────┤
 *                             │                          │
 *                    Iceberg(recommendations)   Kinesis(product-recommendations)
 * </pre>
 *
 * <h3>Broadcast join rationale</h3>
 * <p>The product catalogue is small (~10 k products) and changes infrequently.
 * Broadcasting it means every task manager holds a local copy — recommendation
 * scoring requires no cross-partition communication.
 *
 * <h3>Running</h3>
 * <pre>
 *   flink run -c com.nuskin.prysm.flink.job.RecommendationJob \
 *       target/flink-prysm-pipeline-1.0.0-fat.jar \
 *       --job recommendation
 * </pre>
 */
public class RecommendationJob {

    private static final Logger LOG = LoggerFactory.getLogger(RecommendationJob.class);

    public static void main(String[] args) throws Exception {
        new RecommendationJob().run();
    }

    public void run() throws Exception {
        AppConfig appConfig = AppConfig.load();
        StreamTopologyConfig topology = appConfig.resolveStreamTopology();

        StreamExecutionEnvironment env = buildEnv(appConfig);
        KinesisSourceBuilder sourceBuilder = new KinesisSourceBuilder(appConfig, topology);

        // ── 1. Scan streams → SkinHealthSnapshot (reuse ScanAnalyticsJob logic) ──
        DataStream<RawStreamEvent> scanUnion = null;
        for (StreamDefinition def : topology.getStreams()) {
            if (!def.isScanStream()) continue;
            DataStream<RawStreamEvent> single = sourceBuilder.buildSingleSource(env, def);
            scanUnion = scanUnion == null ? single : scanUnion.union(single);
        }
        if (scanUnion == null) throw new IllegalStateException("No scan streams found");

        DataStream<SkinHealthSnapshot> snapshots = scanUnion
                .keyBy((KeySelector<RawStreamEvent, String>) event -> {
                    Object scanId = event.getField("scanId");
                    return scanId != null ? scanId.toString() : event.getCustomerId();
                })
                .process(new ScanMetricsAggregator())
                .name("ScanMetricsAggregator")
                .uid("rec-scan-aggregator");

        // ── 2. Product catalogue → broadcast stream ───────────────────────────
        StreamDefinition catalogDef = topology.getStreams().stream()
                .filter(d -> "product-catalog".equals(d.getKey()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "product-catalog stream not found in topology"));

        DataStream<RawStreamEvent> catalogStream =
                sourceBuilder.buildSingleSource(env, catalogDef);

        // Extract GenericRecord from the envelope for the broadcast
        DataStream<GenericRecord> catalogRecords = catalogStream
                .map(RawStreamEvent::getAvroRecord)
                .name("catalog-unwrap")
                .uid("catalog-unwrap");

        BroadcastStream<GenericRecord> broadcastCatalog = catalogRecords
                .broadcast(RecommendationEngine.CATALOG_STATE_DESCRIPTOR);

        // ── 3. Keyed scan snapshots ⊕ broadcast catalogue → recommendations ─
        DataStream<RecommendationOutput> recommendations = snapshots
                .keyBy(SkinHealthSnapshot::getCustomerId)
                .connect(broadcastCatalog)
                .process(new RecommendationEngine(appConfig))
                .name("RecommendationEngine")
                .uid("recommendation-engine")
                .setParallelism(appConfig.getDefaultParallelism());

        // ── 4. Log output (attach Iceberg / Kinesis producer sinks here) ──────
        recommendations
                .map(rec -> {
                    LOG.info("Recommendation: customerId={}, products={}",
                             rec.getCustomerId(), rec.getProducts().size());
                    return rec;
                })
                .name("log-recommendations")
                .uid("log-recommendations");

        // TODO (next iteration): Attach Iceberg sink to write recommendations to
        //   scan.product_recommendations table, and/or a Kinesis producer sink
        //   to publish back to the product-recommendations Kinesis stream.

        String jobName = "prysm-recommendation-" + appConfig.getEnv();
        LOG.info("Submitting: {}", jobName);
        env.execute(jobName);
    }

    private StreamExecutionEnvironment buildEnv(AppConfig appConfig) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(appConfig.getDefaultParallelism());
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.enableCheckpointing(appConfig.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointTimeout(300_000L);
        cc.setMinPauseBetweenCheckpoints(5_000L);
        cc.setCheckpointStorage(appConfig.getCheckpointUri());
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        return env;
    }
}
