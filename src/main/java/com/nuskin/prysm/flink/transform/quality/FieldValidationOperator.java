package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.FieldValidation;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.OnFail;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Evaluates every rule in {@code quality.fieldValidations} against the record.
 *
 * <h3>Supported rule types</h3>
 * <ul>
 *   <li>{@code REGEX} — field value must match the configured regular expression</li>
 *   <li>{@code RANGE} — numeric field value must fall within [min, max]</li>
 *   <li>{@code NOT_EMPTY} — field value must not be null or blank</li>
 *   <li>{@code ENUM_VALUE} — field value must be one of {@code allowedValues}</li>
 * </ul>
 *
 * <h3>onFail actions</h3>
 * <ul>
 *   <li>{@code DLQ} — emit to DLQ side-output and suppress from main stream</li>
 *   <li>{@code FILL_DEFAULT} — replace with {@code defaultValue} and forward</li>
 *   <li>{@code DROP_RECORD} — suppress from main stream (no DLQ entry)</li>
 *   <li>{@code FORWARD} — log and forward unchanged</li>
 * </ul>
 *
 * <p>Regex patterns are compiled once at operator open time (not per record).
 */
public class FieldValidationOperator extends ProcessFunction<RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FieldValidationOperator.class);
    private static final String OPERATOR = "FieldValidationOperator";

    private final List<FieldValidation> rules;
    private final String streamKey;

    // Compiled regex patterns — keyed by field name for fast lookup
    private transient Map<String, Pattern> compiledPatterns;

    public FieldValidationOperator(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.rules = (qc != null && qc.getFieldValidations() != null)
                ? qc.getFieldValidations() : List.of();
        this.streamKey = def.getKey();
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        compiledPatterns = new HashMap<>();
        for (FieldValidation rule : rules) {
            if (rule.getPattern() != null && !rule.getPattern().isBlank()) {
                try {
                    compiledPatterns.put(rule.getField(), Pattern.compile(rule.getPattern()));
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Invalid regex pattern for field '" + rule.getField()
                            + "' in stream '" + streamKey + "': " + rule.getPattern(), e);
                }
            }
        }
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<RawStreamEvent> out) {
        for (FieldValidation rule : rules) {
            if (!evaluate(rule, event, ctx)) {
                // evaluate() already handled DLQ / drop according to onFail
                return; // suppress main-stream output
            }
        }
        out.collect(event);
    }

    /**
     * Evaluates a single rule against the event.
     *
     * @return {@code true} if the record should continue to the next rule;
     *         {@code false} if it should be suppressed from the main stream
     */
    private boolean evaluate(FieldValidation rule, RawStreamEvent event, Context ctx) {
        Object raw = event.getField(rule.getField());
        boolean passes = check(rule, raw);
        if (passes) return true;

        OnFail action = rule.getOnFail() != null ? rule.getOnFail() : OnFail.DLQ;
        String detail = "Field '" + rule.getField() + "' failed " + rule.getRule()
                + " (value=" + raw + ")";

        switch (action) {
            case DLQ:
                ctx.output(QualityChainBuilder.DLQ_TAG,
                        new FailedEvent(streamKey, OPERATOR,
                                rule.getRule().name() + "_FAIL", detail, event));
                return false;

            case FILL_DEFAULT:
                if (rule.getDefaultValue() != null) {
                    event.getAvroRecord().put(rule.getField(), rule.getDefaultValue());
                    LOG.debug("Field '{}' validation failed — filled default '{}' for '{}'",
                            rule.getField(), rule.getDefaultValue(), streamKey);
                }
                return true; // continue — filled value may still fail later rules

            case DROP_RECORD:
                LOG.debug("Field '{}' validation failed — dropping record for '{}'",
                        rule.getField(), streamKey);
                return false;

            case FORWARD:
                LOG.warn("Field '{}' validation failed — forwarding anyway for '{}': {}",
                        rule.getField(), streamKey, detail);
                return true;

            default:
                return true;
        }
    }

    private boolean check(FieldValidation rule, Object value) {
        switch (rule.getRule()) {
            case NOT_EMPTY:
                return value != null && !value.toString().isBlank();

            case REGEX:
                if (value == null) return false;
                Pattern p = compiledPatterns.get(rule.getField());
                return p != null && p.matcher(value.toString()).matches();

            case RANGE:
                if (value == null) return false;
                try {
                    double d = Double.parseDouble(value.toString());
                    boolean ok = true;
                    if (rule.getMin() != null) ok = ok && d >= rule.getMin();
                    if (rule.getMax() != null) ok = ok && d <= rule.getMax();
                    return ok;
                } catch (NumberFormatException e) {
                    return false;
                }

            case ENUM_VALUE:
                if (value == null) return false;
                List<String> allowed = rule.getAllowedValues();
                return allowed != null && allowed.contains(value.toString());

            default:
                return true;
        }
    }
}
