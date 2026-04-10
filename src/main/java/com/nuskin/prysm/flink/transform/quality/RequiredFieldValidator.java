package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Validates that all fields listed in {@code quality.requiredFields} are
 * present and non-null in the incoming record.
 *
 * <p>Records that pass all checks are forwarded on the main output.
 * Records with any missing required field are emitted to the DLQ side-output
 * ({@link QualityChainBuilder#DLQ_TAG}) and suppressed from the main stream.
 *
 * <p>Configuration is read entirely from the stream's {@link QualityConfig};
 * no per-stream subclass is needed.
 */
public class RequiredFieldValidator
        extends ProcessFunction<RawStreamEvent, RawStreamEvent> {

    private static final String OPERATOR = "RequiredFieldValidator";

    private final List<String> requiredFields;
    private final String streamKey;

    public RequiredFieldValidator(StreamDefinition def) {
        QualityConfig qc = def.getQuality();
        this.requiredFields = (qc != null && qc.hasRequiredFields())
                ? qc.getRequiredFields()
                : List.of();
        this.streamKey = def.getKey();
    }

    @Override
    public void processElement(RawStreamEvent event,
                               Context ctx,
                               Collector<RawStreamEvent> out) {
        for (String field : requiredFields) {
            Object value = event.getField(field);
            if (value == null || value.toString().isBlank()) {
                ctx.output(QualityChainBuilder.DLQ_TAG,
                        new FailedEvent(
                                streamKey,
                                OPERATOR,
                                "MISSING_REQUIRED_FIELD",
                                "Field '" + field + "' is null or blank",
                                event));
                return; // fail fast — do not emit to main stream
            }
        }
        out.collect(event);
    }
}
