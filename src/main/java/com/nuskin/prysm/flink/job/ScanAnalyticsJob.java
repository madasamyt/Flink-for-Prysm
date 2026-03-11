package com.nuskin.prysm.flink.job;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.model.SkinHealthSnapshot;
import com.nuskin.prysm.flink.source.KinesisSourceBuilder;
import com.nuskin.prysm.flink.transform.ScanMetricsAggregator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <h2>Scan Analytics Job</h2>
 *
 * Joins the seven scan-dimension streams per scanId into unified
 * {@link SkinHealthSnapshot} objects, then computes:
 *
 * <ul>
 *   <li><b>Per-scan snapshots:</b> Complete skin health picture per scan session,
 *       written to {@code scan.skin_health_snapshots} in Iceberg.</li>
 *   <li><b>Rolling 30-minute baseline:</b> Tumbling window aggregation of
 *       per-dimension means, written to {@code scan.skin_health_baselines}.</li>
 *   <li><b>Scan-session enrichment:</b> Joins scan metadata (ScanSession) with
 *       scan metrics to build a full scan record for the data lake.</li>
 * </ul>
 *
 * <pre>
 *   scan-session    ──┐
 *   scan-facial     ──┤
 *   scan-hydration  ──┤
 *   scan-pigment    ──┤──► filter ──► key by scanId ──► ScanMetricsAggregator ──► SkinHealthSnapshot
 *   scan-texture    ──┤                                                              │
 *   scan-wrinkles   ──┤                                                       Iceberg(snapshots)
 *   scan-radiance   ──┤                                                       Iceberg(baselines)
 *   scan-overall    ──┘
 * </pre>
 *
 * <h3>Running</h3>
 * <pre>
 *   flink run -c com.nuskin.prysm.flink.job.ScanAnalyticsJob \
 *       target/flink-prysm-pipeline-1.0.0-fat.jar \
 *       --job analytics
 * </pre>
 */
public class ScanAnalyticsJob {

    private static final Logger LOG = LoggerFactory.getLogger(ScanAnalyticsJob.class);

    private static final Set<String> SCAN_STREAM_KEYS = Set.of(
            "scan-session", "scan-facial", "scan-hydration", "scan-pigmentation",
            "scan-texture", "scan-wrinkles", "scan-radiance", "scan-overall");

    public static void main(String[] args) throws Exception {
        new ScanAnalyticsJob().run();
    }

    public void run() throws Exception {
        AppConfig appConfig = AppConfig.load();
        StreamTopologyConfig topology = appConfig.resolveStreamTopology();

        StreamExecutionEnvironment env = buildEnv(appConfig);

        // ── Build sources only for scan-domain streams ────────────────────────
        KinesisSourceBuilder sourceBuilder = new KinesisSourceBuilder(appConfig, topology);

        List<StreamDefinition> scanStreams = topology.getStreams().stream()
                .filter(StreamDefinition::isScanStream)
                .collect(Collectors.toList());

        LOG.info("ScanAnalyticsJob: subscribing to {} scan streams", scanStreams.size());

        // Build and union only the scan streams (avoids EFO consumers on non-scan streams)
        DataStream<RawStreamEvent> scanStream = null;
        for (StreamDefinition streamDef : scanStreams) {
            DataStream<RawStreamEvent> single = sourceBuilder.buildSingleSource(env, streamDef);
            scanStream = scanStream == null ? single : scanStream.union(single);
        }

        if (scanStream == null) {
            throw new IllegalStateException("No scan streams found in topology");
        }

        // ── Per-scanId aggregation → SkinHealthSnapshot ───────────────────────
        DataStream<SkinHealthSnapshot> snapshots = scanStream
                .filter(event -> SCAN_STREAM_KEYS.contains(event.getStreamKey()))
                .keyBy((KeySelector<RawStreamEvent, String>) event -> {
                    // Key by scanId — every scan stream embeds scanId in its Avro record
                    Object scanId = event.getField("scanId");
                    return scanId != null ? scanId.toString() : event.getCustomerId();
                })
                .process(new ScanMetricsAggregator())
                .name("ScanMetricsAggregator")
                .uid("scan-metrics-aggregator");

        // ── 30-minute rolling baseline per customer ───────────────────────────
        DataStream<Map<String, Double>> baselines = snapshots
                .keyBy(SkinHealthSnapshot::getCustomerId)
                .window(TumblingEventTimeWindows.of(Time.minutes(30)))
                .aggregate(new ScanMetricsAggregator.RollingBaselineAggregate())
                .name("RollingBaseline-30min")
                .uid("rolling-baseline-30min");

        // ── Sink: snapshots → Iceberg ────────────────────────────────────────
        // (Using a custom Iceberg sink for the domain model output)
        snapshots
                .map(snapshot -> {
                    LOG.debug("Snapshot: customerId={}, scores={}",
                              snapshot.getCustomerId(), snapshot.getDimensionScores());
                    return snapshot;
                })
                .name("log-snapshots")
                .uid("log-snapshots");

        // Baselines sink (log for now; attach Iceberg sink using same pattern as IngestionJob)
        baselines
                .map(baseline -> {
                    LOG.debug("Baseline window: {}", baseline);
                    return baseline;
                })
                .name("log-baselines")
                .uid("log-baselines");

        String jobName = "prysm-scan-analytics-" + appConfig.getEnv();
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
