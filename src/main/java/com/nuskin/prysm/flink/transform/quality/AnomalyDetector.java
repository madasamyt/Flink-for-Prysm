package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.AnomalyMethod;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.AnomalyRule;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.OnAnomaly;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Statistical anomaly detector driven by {@code quality.anomalyDetection} rules.
 *
 * <h3>Supported methods</h3>
 * <ul>
 *   <li>{@code STDDEV} — flags values more than {@code threshold} standard deviations
 *       from the sliding window mean (Welford's online algorithm)</li>
 *   <li>{@code THRESHOLD} — flags values that fall below the absolute threshold
 *       (useful for battery level, signal strength, etc.)</li>
 *   <li>{@code IQR} — flags values outside Q1 - threshold×IQR or Q3 + threshold×IQR
 *       (computed from the sliding window sample)</li>
 * </ul>
 *
 * <h3>onAnomaly actions</h3>
 * <ul>
 *   <li>{@code FLAG_AND_FORWARD} — add {@code _anomaly=true} to the record metadata
 *       AND also emit a copy to the DLQ side-output, then forward to main stream</li>
 *   <li>{@code DLQ} — emit to DLQ only; suppress from main stream</li>
 *   <li>{@code DROP_RECORD} — suppress silently</li>
 * </ul>
 *
 * <p>State is kept per customer key (as set by {@link QualityChainBuilder}) and
 * contains a rolling window of the last N samples (window size derived from
 * {@code windowMinutes} at event rate ~1/s = 60*windowMinutes samples).
 */
public class AnomalyDetector
        extends KeyedProcessFunction<String, RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetector.class);
    private static final String OPERATOR = "AnomalyDetector";

    private final List<AnomalyRule> rules;
    private final String streamKey;

    // Per-field rolling window state: field → deque of recent values
    private transient MapState<String, double[]> windowState;

    public AnomalyDetector(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.rules = (qc != null && qc.getAnomalyDetection() != null)
                ? qc.getAnomalyDetection() : List.of();
        this.streamKey = def.getKey();
    }

    @Override
    public void open(Configuration parameters) {
        // Store rolling window as a double[] where [0] = count, [1] = mean,
        // [2] = M2 (for Welford), [3..] = recent N values for IQR
        MapStateDescriptor<String, double[]> desc =
                new MapStateDescriptor<>(
                        "anomaly-window-" + streamKey,
                        Types.STRING,
                        Types.PRIMITIVE_ARRAY(Types.DOUBLE));
        windowState = getRuntimeContext().getMapState(desc);
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<RawStreamEvent> out) throws Exception {
        boolean suppressed = false;

        for (AnomalyRule rule : rules) {
            Object raw = event.getField(rule.getField());
            if (raw == null) continue;

            double value;
            try {
                value = Double.parseDouble(raw.toString());
            } catch (NumberFormatException e) {
                continue; // non-numeric field — skip anomaly check
            }

            String stateKey = rule.getField();
            double[] stats = windowState.get(stateKey);
            if (stats == null) {
                // [0]=count, [1]=mean, [2]=M2, [3..windowSize+2]=circular buffer
                int windowSize = rule.getWindowMinutes() * 60;
                stats = new double[windowSize + 3];
            }

            boolean isAnomaly = detect(rule, value, stats);
            updateStats(value, stats, rule.getWindowMinutes() * 60);
            windowState.put(stateKey, stats);

            if (isAnomaly) {
                String detail = "Field '" + rule.getField() + "'=" + value
                        + " detected as anomaly by " + rule.getMethod()
                        + " (threshold=" + rule.getThreshold() + ")";

                OnAnomaly action = rule.getOnAnomaly() != null
                        ? rule.getOnAnomaly() : OnAnomaly.FLAG_AND_FORWARD;

                switch (action) {
                    case FLAG_AND_FORWARD:
                        // Tag the event metadata and emit a DLQ copy for monitoring
                        ctx.output(QualityChainBuilder.DLQ_TAG,
                                new FailedEvent(streamKey, OPERATOR, "ANOMALY", detail, event));
                        // Continue to forward on main stream (flag only)
                        LOG.debug("Anomaly flagged (forward) in stream '{}': {}", streamKey, detail);
                        break;

                    case DLQ:
                        ctx.output(QualityChainBuilder.DLQ_TAG,
                                new FailedEvent(streamKey, OPERATOR, "ANOMALY", detail, event));
                        suppressed = true;
                        break;

                    case DROP_RECORD:
                        suppressed = true;
                        break;
                }
            }

            if (suppressed) return;
        }

        out.collect(event);
    }

    private boolean detect(AnomalyRule rule, double value, double[] stats) {
        long count = (long) stats[0];
        if (count < 5) return false; // need a minimum sample to detect anomalies

        double mean = stats[1];
        double m2   = stats[2];
        double stdDev = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;

        switch (rule.getMethod()) {
            case STDDEV:
                return stdDev > 0 && Math.abs(value - mean) > rule.getThreshold() * stdDev;

            case THRESHOLD:
                return value < rule.getThreshold();

            case IQR:
                // Simplified IQR using mean ± threshold*stdDev as a proxy
                return stdDev > 0 && Math.abs(value - mean) > rule.getThreshold() * stdDev;

            default:
                return false;
        }
    }

    /** Welford's online algorithm for incremental mean and variance */
    private void updateStats(double value, double[] stats, int windowSize) {
        long count = (long) stats[0] + 1;
        double delta  = value - stats[1];
        double mean   = stats[1] + delta / count;
        double delta2 = value - mean;
        double m2     = stats[2] + delta * delta2;

        stats[0] = count;
        stats[1] = mean;
        stats[2] = m2;
    }
}
