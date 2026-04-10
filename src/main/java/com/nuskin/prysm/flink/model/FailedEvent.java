package com.nuskin.prysm.flink.model;

import java.io.Serializable;

/**
 * Dead-letter event emitted by any quality operator when a record fails
 * validation, deduplication, or passes an anomaly threshold.
 *
 * <p>Carries enough context to:
 * <ul>
 *   <li>Identify the source stream and failing operator</li>
 *   <li>Reconstruct or inspect the original bytes</li>
 *   <li>Drive automated reprocessing via {@code DLQReplayJob}</li>
 * </ul>
 *
 * <p>Failed events are routed via Flink side-output tags defined in
 * {@link com.nuskin.prysm.flink.transform.quality.QualityChainBuilder}.
 */
public class FailedEvent implements Serializable {

    /** Logical stream key from {@code streams-topology.yml} */
    private final String streamKey;

    /** Name of the operator that rejected or flagged this record */
    private final String operatorName;

    /** Short failure category (e.g. MISSING_REQUIRED_FIELD, REGEX_FAIL, ANOMALY) */
    private final String failureReason;

    /** Human-readable detail message */
    private final String message;

    /** The original RawStreamEvent — preserved for replay / inspection */
    private final RawStreamEvent originalEvent;

    /** Wall-clock time when the event was rejected (epoch ms) */
    private final long failureTimeMs;

    public FailedEvent(String streamKey,
                       String operatorName,
                       String failureReason,
                       String message,
                       RawStreamEvent originalEvent) {
        this.streamKey     = streamKey;
        this.operatorName  = operatorName;
        this.failureReason = failureReason;
        this.message       = message;
        this.originalEvent = originalEvent;
        this.failureTimeMs = System.currentTimeMillis();
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    public String getStreamKey()       { return streamKey; }
    public String getOperatorName()    { return operatorName; }
    public String getFailureReason()   { return failureReason; }
    public String getMessage()         { return message; }
    public RawStreamEvent getOriginalEvent() { return originalEvent; }
    public long getFailureTimeMs()     { return failureTimeMs; }

    @Override
    public String toString() {
        return "FailedEvent{streamKey='" + streamKey
                + "', operator='" + operatorName
                + "', reason='" + failureReason
                + "', msg='" + message + "'}";
    }
}
