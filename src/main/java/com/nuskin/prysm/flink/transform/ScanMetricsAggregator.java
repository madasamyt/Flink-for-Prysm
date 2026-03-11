package com.nuskin.prysm.flink.transform;

import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.model.SkinHealthSnapshot;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregates the seven scan-dimension streams for a single {@code scanId} into
 * a unified {@link SkinHealthSnapshot}.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>All seven scan-dimension events (hydration, pigmentation, texture, wrinkles,
 *       radiance, facial, overall) are keyed by {@code scanId}.</li>
 *   <li>As each event arrives, its metrics are merged into a per-scanId
 *       {@link MapState}.</li>
 *   <li>A processing-time timer fires 60 seconds after the first event for a
 *       scanId. At that point the accumulated state is emitted as a
 *       {@link SkinHealthSnapshot} regardless of how many dimensions arrived.</li>
 *   <li>If the {@code scan-overall} event arrives (the authoritative completion
 *       signal from the device pipeline), the snapshot is emitted immediately
 *       without waiting for the timer.</li>
 * </ol>
 *
 * <p>This approach is resilient to individual dimension streams being delayed
 * or missing (e.g. a partial scan).
 */
public class ScanMetricsAggregator
        extends KeyedProcessFunction<String, RawStreamEvent, SkinHealthSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(ScanMetricsAggregator.class);

    /** Wait at most 60 s after first event before emitting the snapshot */
    private static final long AGGREGATION_TIMEOUT_MS = 60_000L;

    // Keyed by scanId
    private transient MapState<String, Double> dimensionScores;
    private transient MapState<String, Object> rawMetrics;
    private transient ValueState<Long>         timerTs;
    private transient ValueState<String>       customerId;

    @Override
    public void open(Configuration parameters) {
        dimensionScores = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("dimensionScores", String.class, Double.class));

        rawMetrics = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("rawMetrics", String.class, Object.class));

        timerTs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerTs", Long.class));

        customerId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customerId", String.class));
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<SkinHealthSnapshot> out) throws Exception {
        String scanId = getCurrentKey();
        GenericRecord rec = event.getAvroRecord();

        // Register customerId on first event for this scanId
        if (customerId.value() == null) {
            customerId.update(event.getCustomerId());
        }

        // Merge dimension scores into state
        extractAndMergeScores(event, rec);

        // Set a processing-time timer on first arrival if not already set
        if (timerTs.value() == null) {
            long fireAt = ctx.timerService().currentProcessingTime() + AGGREGATION_TIMEOUT_MS;
            ctx.timerService().registerProcessingTimeTimer(fireAt);
            timerTs.update(fireAt);
        }

        // Emit immediately when the authoritative overall-scan event arrives
        if ("scan-overall".equals(event.getStreamKey())) {
            emitSnapshot(scanId, out);
            clearState(ctx);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<SkinHealthSnapshot> out) throws Exception {
        String scanId = getCurrentKey();
        LOG.debug("Aggregation timer fired for scanId={}", scanId);
        emitSnapshot(scanId, out);
        clearState(ctx);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void extractAndMergeScores(RawStreamEvent event, GenericRecord rec) throws Exception {
        String streamKey = event.getStreamKey();
        switch (streamKey) {
            case "scan-hydration":
                putIfNotNull("hydration", rec, "overallHydrationScore");
                break;
            case "scan-pigmentation":
                putIfNotNull("pigmentation", rec, "overallPigmentationScore");
                putIfNotNull("darkSpotsCount", rec, "darkSpotsCount");
                break;
            case "scan-texture":
                putIfNotNull("texture", rec, "overallTextureScore");
                putIfNotNull("poreScore", rec, "poreScore");
                break;
            case "scan-wrinkles":
                putIfNotNull("wrinkle", rec, "overallWrinkleScore");
                putIfNotNull("skinAgeEstimate", rec, "estimatedSkinAge");
                break;
            case "scan-radiance":
                putIfNotNull("radiance", rec, "radianceScore");
                putIfNotNull("clarity", rec, "clarityScore");
                break;
            case "scan-facial":
                putIfNotNull("symmetry", rec, "symmetryScore");
                // Store skin tone for personalisation
                Object skinTone = rec.getSchema().getField("skinToneLabel") != null
                        ? rec.get("skinToneLabel") : null;
                if (skinTone != null) {
                    rawMetrics.put("skinToneLabel", skinTone.toString());
                }
                break;
            case "scan-overall":
                putIfNotNull("overall", rec, "overallSkinScore");
                Object topConcerns = rec.get("topConcerns");
                if (topConcerns != null) {
                    rawMetrics.put("topConcerns", topConcerns.toString());
                }
                break;
            default:
                LOG.trace("Ignoring non-scan stream in aggregator: {}", streamKey);
        }
    }

    private void putIfNotNull(String key, GenericRecord rec, String fieldName) throws Exception {
        if (rec.getSchema().getField(fieldName) == null) return;
        Object val = rec.get(fieldName);
        if (val == null) return;
        double d = ((Number) val).doubleValue();
        dimensionScores.put(key, d);
    }

    private void emitSnapshot(String scanId, Collector<SkinHealthSnapshot> out) throws Exception {
        Map<String, Double> scores = new HashMap<>();
        dimensionScores.entries().forEach(e -> scores.put(e.getKey(), e.getValue()));

        Map<String, String> extras = new HashMap<>();
        rawMetrics.entries().forEach(e -> extras.put(e.getKey(), String.valueOf(e.getValue())));

        SkinHealthSnapshot snapshot = new SkinHealthSnapshot(
                customerId.value(),
                scanId,
                Instant.now().toEpochMilli(),
                scores,
                extras);

        out.collect(snapshot);
        LOG.debug("Emitted SkinHealthSnapshot for scanId={}, dimensions={}",
                  scanId, scores.keySet());
    }

    private void clearState(KeyedProcessFunction<?, ?, ?>.Context ctx) throws Exception {
        dimensionScores.clear();
        rawMetrics.clear();
        Long ts = timerTs.value();
        if (ts != null) {
            ctx.timerService().deleteProcessingTimeTimer(ts);
        }
        timerTs.clear();
        customerId.clear();
    }

    private void clearState(KeyedProcessFunction<?, ?, ?>.OnTimerContext ctx) throws Exception {
        dimensionScores.clear();
        rawMetrics.clear();
        timerTs.clear();
        customerId.clear();
    }

    // ── Static aggregate function (for windowed rollups) ─────────────────────

    /**
     * Windowed aggregate that computes the mean of each skin dimension score
     * over a tumbling window, producing a per-customer rolling baseline.
     * Used in the 30-minute rolling window aggregation for trend analysis.
     */
    public static class RollingBaselineAggregate
            implements AggregateFunction<SkinHealthSnapshot,
                                         Map<String, Double>,
                                         Map<String, Double>> {

        @Override
        public Map<String, Double> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, Double> add(SkinHealthSnapshot snapshot, Map<String, Double> acc) {
            snapshot.getDimensionScores().forEach((k, v) -> {
                // Running sum — will be divided by count in getResult
                acc.merge(k + "_sum", v, Double::sum);
                acc.merge(k + "_count", 1.0, Double::sum);
            });
            return acc;
        }

        @Override
        public Map<String, Double> getResult(Map<String, Double> acc) {
            Map<String, Double> means = new HashMap<>();
            acc.entrySet().stream()
               .filter(e -> e.getKey().endsWith("_sum"))
               .forEach(e -> {
                   String dim = e.getKey().replace("_sum", "");
                   Double count = acc.get(dim + "_count");
                   if (count != null && count > 0) {
                       means.put(dim, e.getValue() / count);
                   }
               });
            return means;
        }

        @Override
        public Map<String, Double> merge(Map<String, Double> a, Map<String, Double> b) {
            Map<String, Double> merged = new HashMap<>(a);
            b.forEach((k, v) -> merged.merge(k, v, Double::sum));
            return merged;
        }
    }
}
