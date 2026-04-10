package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.NullStrategy;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Applies the {@code quality.nullStrategy} and {@code quality.fieldDefaults}
 * rules to handle null field values after required-field validation.
 *
 * <h3>Strategies</h3>
 * <ul>
 *   <li>{@code FORWARD} (default) — forward the record unchanged; nulls pass through</li>
 *   <li>{@code FILL_DEFAULT} — replace nulls with the value from
 *       {@code quality.fieldDefaults}; fields without a configured default are
 *       forwarded as null</li>
 *   <li>{@code DROP_RECORD} — any null non-required field causes the entire
 *       record to be sent to the DLQ and suppressed from the main stream</li>
 * </ul>
 *
 * <p>Required fields were already checked by {@link RequiredFieldValidator};
 * this operator acts on all other nullable fields.
 */
public class NullHandlerOperator extends ProcessFunction<RawStreamEvent, RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(NullHandlerOperator.class);
    private static final String OPERATOR = "NullHandlerOperator";

    private final NullStrategy nullStrategy;
    private final Map<String, String> fieldDefaults;
    private final String streamKey;

    public NullHandlerOperator(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.nullStrategy   = (qc != null) ? qc.getNullStrategy() : NullStrategy.FORWARD;
        this.fieldDefaults  = (qc != null && qc.getFieldDefaults() != null)
                ? qc.getFieldDefaults() : Map.of();
        this.streamKey = def.getKey();
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<RawStreamEvent> out) {

        if (nullStrategy == NullStrategy.FORWARD) {
            out.collect(event);
            return;
        }

        for (org.apache.avro.Schema.Field field : event.getAvroRecord().getSchema().getFields()) {
            String fieldName = field.name();
            Object value = event.getAvroRecord().get(fieldName);

            if (value != null) continue; // not null — nothing to do

            switch (nullStrategy) {
                case FILL_DEFAULT:
                    String defaultVal = fieldDefaults.get(fieldName);
                    if (defaultVal != null) {
                        event.getAvroRecord().put(fieldName, defaultVal);
                        LOG.debug("Filled null field '{}' with default '{}' for stream '{}'",
                                fieldName, defaultVal, streamKey);
                    }
                    // If no default configured: forward null as-is
                    break;

                case DROP_RECORD:
                    // Only drop if there's no default configured
                    if (!fieldDefaults.containsKey(fieldName)) {
                        ctx.output(QualityChainBuilder.DLQ_TAG,
                                new FailedEvent(
                                        streamKey,
                                        OPERATOR,
                                        "NULL_FIELD_DROP",
                                        "Field '" + fieldName + "' is null and strategy is DROP_RECORD",
                                        event));
                        return; // suppress from main stream
                    } else {
                        // Has default — fill it
                        event.getAvroRecord().put(fieldName, fieldDefaults.get(fieldName));
                    }
                    break;

                default:
                    break;
            }
        }

        out.collect(event);
    }
}
