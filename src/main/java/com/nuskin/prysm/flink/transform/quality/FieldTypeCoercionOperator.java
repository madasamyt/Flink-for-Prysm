package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.CoercionType;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.TypeCoercion;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Applies the {@code quality.typeCoercions} rules declared in
 * {@code streams-topology.yml} to each event.
 *
 * <p>Supported coercions (from → to):
 * <ul>
 *   <li>{@code ISO8601_STRING → EPOCH_MILLIS} — parses an ISO-8601 timestamp
 *       string and writes back the epoch-millisecond long value</li>
 *   <li>{@code LOWERCASE_STRING → UPPERCASE_ENUM} — upper-cases a string
 *       field for enum normalisation</li>
 *   <li>{@code TRIM_STRING} — trims leading/trailing whitespace</li>
 *   <li>{@code NUMERIC_STRING} — strips non-numeric characters from a
 *       string that represents a number</li>
 * </ul>
 *
 * <p>Unknown field names are silently skipped (schema-drift safe).
 * Coercion failures (e.g., unparseable date string) are logged at WARN level
 * and the original value is forwarded unchanged — this prevents a bad value
 * from cascading to a DLQ; field validation operators handle the semantic
 * check after coercions are applied.
 */
public class FieldTypeCoercionOperator extends RichMapFunction<RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FieldTypeCoercionOperator.class);

    private final List<TypeCoercion> coercions;

    public FieldTypeCoercionOperator(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.coercions = (qc != null && qc.getTypeCoercions() != null)
                ? qc.getTypeCoercions() : List.of();
    }

    @Override
    public RawStreamEvent map(RawStreamEvent event) {
        if (coercions.isEmpty()) return event;

        GenericRecord rec = event.getAvroRecord();
        for (TypeCoercion rule : coercions) {
            applyCoercion(rec, rule);
        }
        return event;
    }

    private void applyCoercion(GenericRecord rec, TypeCoercion rule) {
        if (rec.getSchema().getField(rule.getField()) == null) return; // schema-drift guard

        Object value = rec.get(rule.getField());
        if (value == null) return;

        CoercionType from = rule.getFromType();
        CoercionType to   = rule.getToType();

        try {
            if (from == CoercionType.ISO8601_STRING && to == CoercionType.EPOCH_MILLIS) {
                String str = value.toString().trim();
                long epochMs = Instant.from(
                        DateTimeFormatter.ISO_DATE_TIME.parse(str)).toEpochMilli();
                rec.put(rule.getField(), epochMs);

            } else if (from == CoercionType.LOWERCASE_STRING
                    && to == CoercionType.UPPERCASE_ENUM) {
                rec.put(rule.getField(),
                        new GenericData.EnumSymbol(
                                rec.getSchema().getField(rule.getField()).schema(),
                                value.toString().toUpperCase().trim()));

            } else if (from == CoercionType.TRIM_STRING
                    || to == CoercionType.TRIM_STRING) {
                rec.put(rule.getField(), value.toString().trim());

            } else if (from == CoercionType.NUMERIC_STRING
                    || to == CoercionType.NUMERIC_STRING) {
                rec.put(rule.getField(), value.toString().replaceAll("[^0-9.\\-]", ""));
            }
        } catch (Exception e) {
            LOG.warn("Coercion failed for field '{}' ({}→{}): {} — forwarding original value",
                    rule.getField(), from, to, e.getMessage());
        }
    }
}
