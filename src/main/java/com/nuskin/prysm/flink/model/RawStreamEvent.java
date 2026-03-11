package com.nuskin.prysm.flink.model;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.time.Instant;

/**
 * Pipeline-internal envelope wrapping a deserialised Avro {@link GenericRecord}
 * together with its routing metadata.
 *
 * <p>Using an envelope pattern means all 19 source streams can be processed by
 * the same generic pipeline stages (PII encryption, schema drift handling,
 * Iceberg write) before being fanned out into stream-specific processing.
 *
 * <p><b>Note:</b> {@link GenericRecord} is not Java-serialisable. Flink's Kryo
 * serialiser handles it in practice, but for large-scale deployments consider
 * converting to a byte[] + schema ID form before crossing network boundaries.
 */
public class RawStreamEvent implements Serializable {

    /** The logical stream key from {@code streams-topology.yml} */
    private final String streamKey;

    /** Full stream definition (table name, PII fields, etc.) */
    private final StreamDefinition streamDefinition;

    /** The deserialised Avro record — field access is by name (schema-drift safe) */
    private final GenericRecord avroRecord;

    /** Wall-clock time when the event entered the Flink pipeline */
    private final long ingestTimeMs;

    // ── Constructor ──────────────────────────────────────────────────────────

    public RawStreamEvent(String streamKey,
                          StreamDefinition streamDefinition,
                          GenericRecord avroRecord) {
        this.streamKey        = streamKey;
        this.streamDefinition = streamDefinition;
        this.avroRecord       = avroRecord;
        this.ingestTimeMs     = Instant.now().toEpochMilli();
    }

    // ── Convenience helpers ──────────────────────────────────────────────────

    /**
     * Safely reads a field from the underlying GenericRecord.
     * Returns {@code null} when the field does not exist in the current schema
     * version — this is the primary schema-drift guard.
     */
    public Object getField(String fieldName) {
        try {
            if (avroRecord.getSchema().getField(fieldName) == null) {
                return null; // Field added in a future schema version
            }
            return avroRecord.get(fieldName);
        } catch (Exception e) {
            return null;
        }
    }

    /** Returns the customerId field, present in every schema */
    public String getCustomerId() {
        Object id = getField("customerId");
        return id != null ? id.toString() : null;
    }

    /** Returns the event timestamp from the Avro record (epoch ms) */
    public Long getEventTimeMs() {
        Object t = getField("eventTime");
        return t != null ? (Long) t : null;
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    public String getStreamKey() { return streamKey; }
    public StreamDefinition getStreamDefinition() { return streamDefinition; }
    public GenericRecord getAvroRecord() { return avroRecord; }
    public long getIngestTimeMs() { return ingestTimeMs; }

    @Override
    public String toString() {
        return "RawStreamEvent{streamKey='" + streamKey
                + "', customerId=" + getCustomerId()
                + ", eventTime=" + getEventTimeMs() + "}";
    }
}
