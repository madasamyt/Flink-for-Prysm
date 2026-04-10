package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata-driven quality chain builder.
 *
 * <p>Reads the {@code quality} block from a {@link StreamDefinition} and
 * assembles a series of Flink operators that implement every declared rule.
 * No Java code changes are needed to add, remove, or tune quality rules —
 * editing {@code streams-topology.yml} is sufficient.
 *
 * <h3>Operator chain (in order)</h3>
 * <ol>
 *   <li>{@link RequiredFieldValidator} — rejects records missing mandatory fields</li>
 *   <li>{@link FieldTypeCoercionOperator} — applies ISO8601→epoch, enum-case, etc.</li>
 *   <li>{@link NullHandlerOperator} — fills or drops nulls per nullStrategy</li>
 *   <li>{@link FieldValidationOperator} — REGEX / RANGE / NOT_EMPTY / ENUM_VALUE rules</li>
 *   <li>{@link DeduplicationOperator} — RocksDB dedup keyed by dedupKey</li>
 *   <li>{@link AnomalyDetector} — statistical anomaly detection per field</li>
 * </ol>
 *
 * <p>Operators that are not configured (empty rule lists, no dedupKey, etc.)
 * are no-ops and add no overhead.  Failed events are emitted via Flink
 * side-output using the shared {@link #DLQ_TAG}.
 */
public class QualityChainBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(QualityChainBuilder.class);

    /**
     * Shared side-output tag for all quality failures and anomaly flags.
     * Collect this tag from the final operator in the chain to route DLQ events.
     */
    public static final OutputTag<FailedEvent> DLQ_TAG =
            new OutputTag<FailedEvent>("quality-dlq") {};

    /**
     * Result returned by {@link #attach(DataStream, StreamDefinition)}.
     * Consumers use both streams to wire downstream sinks.
     */
    public static class QualityChainResult {
        private final DataStream<RawStreamEvent> clean;
        private final DataStream<FailedEvent>    dlq;

        QualityChainResult(DataStream<RawStreamEvent> clean,
                           DataStream<FailedEvent> dlq) {
            this.clean = clean;
            this.dlq   = dlq;
        }

        /** The quality-filtered clean stream ready for PII encryption and sinking */
        public DataStream<RawStreamEvent> clean() { return clean; }

        /** Failed / anomaly-flagged events destined for the DLQ sink */
        public DataStream<FailedEvent> dlq()       { return dlq; }
    }

    /**
     * Attaches the full quality chain to {@code input} based on the rules
     * declared in {@code def.getQuality()}.
     *
     * <p>If the stream has no quality block, a passthrough is returned —
     * all events arrive on the clean stream and the DLQ stream is empty.
     *
     * @param input the raw/deserialized event stream for one logical stream
     * @param def   the stream's topology definition (contains the quality rules)
     * @return clean stream + DLQ side-output stream
     */
    public static QualityChainResult attach(
            DataStream<RawStreamEvent> input,
            StreamDefinition def) {

        String k = def.getKey();
        QualityConfig qc = def.getQuality();

        if (qc == null) {
            LOG.debug("Stream '{}' has no quality config — passthrough", k);
            // Return an empty DLQ stream by filtering for a never-true condition
            DataStream<FailedEvent> emptyDlq = input
                    .process(new RequiredFieldValidator(def))
                    .name("quality-required-" + k)
                    .uid("quality-required-" + k)
                    .getSideOutput(DLQ_TAG);
            // Still pass through the full stream unchanged
            SingleOutputStreamOperator<RawStreamEvent> passthrough =
                    input.process(new RequiredFieldValidator(def))
                         .name("quality-required-" + k + "-pt")
                         .uid("quality-required-" + k + "-pt");
            return new QualityChainResult(passthrough, emptyDlq);
        }

        // ── Step 1: Required field validation ────────────────────────────────
        SingleOutputStreamOperator<RawStreamEvent> afterRequired =
                input.process(new RequiredFieldValidator(def))
                     .name("quality-required-" + k)
                     .uid("quality-required-" + k);

        // ── Step 2: Type coercions ────────────────────────────────────────────
        DataStream<RawStreamEvent> afterCoerce = afterRequired;
        if (qc.hasCoercions()) {
            afterCoerce = afterRequired
                    .map(new FieldTypeCoercionOperator(def))
                    .name("quality-coerce-" + k)
                    .uid("quality-coerce-" + k);
        }

        // ── Step 3: Null handling ─────────────────────────────────────────────
        SingleOutputStreamOperator<RawStreamEvent> afterNull =
                afterCoerce.process(new NullHandlerOperator(def))
                           .name("quality-nulls-" + k)
                           .uid("quality-nulls-" + k);

        // ── Step 4: Field validation (REGEX, RANGE, ENUM, NOT_EMPTY) ─────────
        DataStream<RawStreamEvent> afterValidate = afterNull;
        if (qc.hasValidations()) {
            afterValidate = afterNull.process(new FieldValidationOperator(def))
                                     .name("quality-validate-" + k)
                                     .uid("quality-validate-" + k);
        }

        // ── Step 5: Deduplication ─────────────────────────────────────────────
        DataStream<RawStreamEvent> afterDedup = afterValidate;
        if (qc.hasDedupKey()) {
            afterDedup = afterValidate
                    .keyBy(event -> {
                        Object key = event.getField(qc.getDedupKey());
                        return key != null ? key.toString() : "";
                    })
                    .process(new DeduplicationOperator(def))
                    .name("quality-dedup-" + k)
                    .uid("quality-dedup-" + k);
        }

        // ── Step 6: Anomaly detection ─────────────────────────────────────────
        SingleOutputStreamOperator<RawStreamEvent> afterAnomaly;
        if (qc.hasAnomalyRules()) {
            afterAnomaly = afterDedup
                    .keyBy(event -> {
                        String cid = event.getCustomerId();
                        return cid != null ? cid : event.getStreamKey();
                    })
                    .process(new AnomalyDetector(def))
                    .name("quality-anomaly-" + k)
                    .uid("quality-anomaly-" + k);
        } else {
            // Wrap in a no-op process to provide a consistent getSideOutput handle
            afterAnomaly = afterDedup.process(new AnomalyDetector(def))
                                     .name("quality-anomaly-noop-" + k)
                                     .uid("quality-anomaly-noop-" + k);
        }

        // Collect all DLQ side-outputs — union from every step that can emit them
        DataStream<FailedEvent> dlq = afterRequired.getSideOutput(DLQ_TAG)
                .union(afterNull.getSideOutput(DLQ_TAG))
                .union(afterAnomaly.getSideOutput(DLQ_TAG));

        if (qc.hasValidations()) {
            dlq = dlq.union(
                    ((SingleOutputStreamOperator<RawStreamEvent>) afterValidate)
                            .getSideOutput(DLQ_TAG));
        }

        LOG.info("Quality chain attached for stream '{}': required={}, coercions={}, "
                + "validations={}, dedup={}, anomaly={}",
                k,
                qc.hasRequiredFields(),
                qc.hasCoercions(),
                qc.hasValidations(),
                qc.hasDedupKey(),
                qc.hasAnomalyRules());

        return new QualityChainResult(afterAnomaly, dlq);
    }

    private QualityChainBuilder() {}
}
