package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stateful deduplication keyed by {@code quality.dedupKey}.
 *
 * <p>Uses a RocksDB-backed {@link ValueState}{@code <Long>} that stores the
 * ingest timestamp of the first occurrence of each key.  State TTL is
 * configured from {@code quality.dedupTtlHours} so memory usage is bounded
 * even for high-cardinality dedup keys.
 *
 * <p>Duplicate records (same key seen within the TTL window) are routed to the
 * DLQ side-output for auditability rather than silently dropped.
 *
 * <p>The stream must be {@code keyBy(dedupKey)} before this operator is wired
 * — {@link QualityChainBuilder} handles this automatically.
 */
public class DeduplicationOperator
        extends KeyedProcessFunction<String, RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationOperator.class);
    private static final String OPERATOR = "DeduplicationOperator";

    private final String dedupKey;
    private final int dedupTtlHours;
    private final String streamKey;

    private transient ValueState<Long> seenState;

    public DeduplicationOperator(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.dedupKey      = qc != null ? qc.getDedupKey() : null;
        this.dedupTtlHours = qc != null ? qc.getDedupTtlHours() : 24;
        this.streamKey     = def.getKey();
    }

    @Override
    public void open(Configuration parameters) {
        org.apache.flink.api.common.state.StateTtlConfig ttlConfig =
                org.apache.flink.api.common.state.StateTtlConfig
                        .newBuilder(Time.hours(dedupTtlHours))
                        .setUpdateType(
                                org.apache.flink.api.common.state.StateTtlConfig
                                        .UpdateType.OnCreateAndWrite)
                        .setStateVisibility(
                                org.apache.flink.api.common.state.StateTtlConfig
                                        .StateVisibility.NeverReturnExpired)
                        .build();

        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("dedup-first-seen-" + streamKey, Types.LONG);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<RawStreamEvent> out) throws Exception {
        Long firstSeen = seenState.value();

        if (firstSeen != null) {
            // Duplicate within TTL window
            LOG.debug("Duplicate detected for key='{}' in stream '{}' (first seen {}ms ago)",
                    getCurrentKey(), streamKey, System.currentTimeMillis() - firstSeen);
            ctx.output(QualityChainBuilder.DLQ_TAG,
                    new FailedEvent(
                            streamKey,
                            OPERATOR,
                            "DUPLICATE_EVENT",
                            "Duplicate " + dedupKey + "='" + getCurrentKey() + "'",
                            event));
            return;
        }

        seenState.update(event.getIngestTimeMs());
        out.collect(event);
    }
}
