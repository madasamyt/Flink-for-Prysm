package com.nuskin.prysm.flink.transform.quality;

import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.FieldValidation;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.IcebergSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.KinesisSourceConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.NullStrategy;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.OnFail;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.QualityConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.SchemaConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.SinkType;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.SourceType;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamType;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.ValidationRule;
import com.nuskin.prysm.flink.model.FailedEvent;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the metadata-driven quality operator chain.
 *
 * <p>Each test builds a minimal {@link StreamDefinition} with the relevant
 * quality config populated and verifies that the operator forwards clean events
 * or routes failures to the DLQ side-output as expected.
 */
class QualityOperatorsTest {

    // ── Test Avro schema ───────────────────────────────────────────────────────

    private static final Schema TEST_SCHEMA = SchemaBuilder.record("TestEvent")
            .namespace("com.nuskin.prysm.test")
            .fields()
            .requiredString("eventId")
            .requiredString("customerId")
            .requiredLong("eventTime")
            .optionalString("email")
            .optionalString("countryCode")
            .optionalDouble("score")
            .optionalString("platform")
            .endRecord();

    // ── Helpers ────────────────────────────────────────────────────────────────

    private GenericRecord record(String eventId, String customerId, Long eventTime) {
        GenericRecord rec = new GenericData.Record(TEST_SCHEMA);
        rec.put("eventId", eventId);
        rec.put("customerId", customerId);
        rec.put("eventTime", eventTime);
        return rec;
    }

    private RawStreamEvent event(GenericRecord rec) {
        return new RawStreamEvent("test-stream", buildStreamDef(null), rec);
    }

    private StreamDefinition buildStreamDef(QualityConfig qc) {
        StreamDefinition def = new StreamDefinition();
        def.setKey("test-stream");
        def.setStreamType(StreamType.CUSTOMER);

        KinesisSourceConfig src = new KinesisSourceConfig();
        src.setType(SourceType.KINESIS);
        src.setStreamNameParamPath("/test/stream");
        src.setEfoConsumer("test-consumer");
        def.setSource(src);

        SchemaConfig schema = new SchemaConfig();
        schema.setAvroFile("customer-profile.avsc");
        schema.setGlueSchemaName("TestEvent");
        schema.setGlueRegistryName("test-registry");
        def.setSchema(schema);

        def.setQuality(qc);

        IcebergSinkConfig sink = new IcebergSinkConfig();
        sink.setType(SinkType.ICEBERG);
        sink.setDatabase("test");
        sink.setTable("test_events");
        def.setSinks(List.of(sink));
        return def;
    }

    // ── Collecting helpers ─────────────────────────────────────────────────────

    static class ListCollector<T> implements Collector<T> {
        final List<T> collected = new ArrayList<>();
        @Override public void collect(T record) { collected.add(record); }
        @Override public void close() {}
    }

    // =========================================================================
    // RequiredFieldValidator tests
    // =========================================================================

    @Nested
    @DisplayName("RequiredFieldValidator")
    class RequiredFieldValidatorTests {

        @Test
        @DisplayName("forwards record when all required fields are present")
        void forwardsWhenAllPresent() throws Exception {
            QualityConfig qc = new QualityConfig();
            qc.setRequiredFields(List.of("eventId", "customerId", "eventTime"));

            RequiredFieldValidator op = new RequiredFieldValidator(buildStreamDef(qc));
            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertTrue(dlq.collected.isEmpty());
        }

        @Test
        @DisplayName("routes to DLQ when required field is null")
        void dlqWhenFieldNull() throws Exception {
            QualityConfig qc = new QualityConfig();
            qc.setRequiredFields(List.of("eventId", "customerId", "eventTime"));

            RequiredFieldValidator op = new RequiredFieldValidator(buildStreamDef(qc));
            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            // customerId = null
            GenericRecord rec = new GenericData.Record(TEST_SCHEMA);
            rec.put("eventId", "e1");
            rec.put("customerId", null);
            rec.put("eventTime", 1000L);
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertTrue(out.collected.isEmpty(), "Record with null required field should not be forwarded");
            assertEquals(1, dlq.collected.size());
            assertEquals("MISSING_REQUIRED_FIELD", dlq.collected.get(0).getFailureReason());
        }

        @Test
        @DisplayName("passes through when no required fields configured")
        void passthroughWhenNoConfig() throws Exception {
            QualityConfig qc = new QualityConfig();
            qc.setRequiredFields(Collections.emptyList());

            RequiredFieldValidator op = new RequiredFieldValidator(buildStreamDef(qc));
            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = new GenericData.Record(TEST_SCHEMA);
            // all fields null — should still pass
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertTrue(dlq.collected.isEmpty());
        }
    }

    // =========================================================================
    // FieldValidationOperator tests
    // =========================================================================

    @Nested
    @DisplayName("FieldValidationOperator")
    class FieldValidationOperatorTests {

        @Test
        @DisplayName("REGEX rule passes valid email")
        void regexPassesValidEmail() throws Exception {
            FieldValidation rule = new FieldValidation();
            rule.setField("email");
            rule.setRule(ValidationRule.REGEX);
            rule.setPattern("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
            rule.setOnFail(OnFail.DLQ);

            QualityConfig qc = new QualityConfig();
            qc.setFieldValidations(List.of(rule));

            FieldValidationOperator op = new FieldValidationOperator(buildStreamDef(qc));
            op.open(new org.apache.flink.configuration.Configuration());

            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("email", "user@example.com");
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertTrue(dlq.collected.isEmpty());
        }

        @Test
        @DisplayName("REGEX rule routes invalid email to DLQ")
        void regexDlqInvalidEmail() throws Exception {
            FieldValidation rule = new FieldValidation();
            rule.setField("email");
            rule.setRule(ValidationRule.REGEX);
            rule.setPattern("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
            rule.setOnFail(OnFail.DLQ);

            QualityConfig qc = new QualityConfig();
            qc.setFieldValidations(List.of(rule));

            FieldValidationOperator op = new FieldValidationOperator(buildStreamDef(qc));
            op.open(new org.apache.flink.configuration.Configuration());

            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("email", "not-an-email");
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertTrue(out.collected.isEmpty());
            assertEquals(1, dlq.collected.size());
            assertEquals("REGEX_FAIL", dlq.collected.get(0).getFailureReason());
        }

        @Test
        @DisplayName("RANGE rule passes score within bounds")
        void rangePassesInBounds() throws Exception {
            FieldValidation rule = new FieldValidation();
            rule.setField("score");
            rule.setRule(ValidationRule.RANGE);
            rule.setMin(0.0);
            rule.setMax(100.0);
            rule.setOnFail(OnFail.DLQ);

            QualityConfig qc = new QualityConfig();
            qc.setFieldValidations(List.of(rule));

            FieldValidationOperator op = new FieldValidationOperator(buildStreamDef(qc));
            op.open(new org.apache.flink.configuration.Configuration());

            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("score", 75.5);
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertTrue(dlq.collected.isEmpty());
        }

        @Test
        @DisplayName("RANGE rule routes out-of-bounds value to DLQ")
        void rangeDlqOutOfBounds() throws Exception {
            FieldValidation rule = new FieldValidation();
            rule.setField("score");
            rule.setRule(ValidationRule.RANGE);
            rule.setMin(0.0);
            rule.setMax(100.0);
            rule.setOnFail(OnFail.DLQ);

            QualityConfig qc = new QualityConfig();
            qc.setFieldValidations(List.of(rule));

            FieldValidationOperator op = new FieldValidationOperator(buildStreamDef(qc));
            op.open(new org.apache.flink.configuration.Configuration());

            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("score", 150.0); // out of range
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertTrue(out.collected.isEmpty());
            assertEquals(1, dlq.collected.size());
        }

        @Test
        @DisplayName("ENUM_VALUE rule with FILL_DEFAULT replaces invalid value and forwards")
        void enumFillDefault() throws Exception {
            FieldValidation rule = new FieldValidation();
            rule.setField("platform");
            rule.setRule(ValidationRule.ENUM_VALUE);
            rule.setAllowedValues(List.of("IOS", "ANDROID", "WEB", "UNKNOWN"));
            rule.setOnFail(OnFail.FILL_DEFAULT);
            rule.setDefaultValue("UNKNOWN");

            QualityConfig qc = new QualityConfig();
            qc.setFieldValidations(List.of(rule));

            FieldValidationOperator op = new FieldValidationOperator(buildStreamDef(qc));
            op.open(new org.apache.flink.configuration.Configuration());

            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("platform", "SMARTTV"); // not in allowed values
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size(), "Record should be forwarded with default");
            assertEquals("UNKNOWN", out.collected.get(0).getField("platform").toString());
            assertTrue(dlq.collected.isEmpty());
        }
    }

    // =========================================================================
    // NullHandlerOperator tests
    // =========================================================================

    @Nested
    @DisplayName("NullHandlerOperator")
    class NullHandlerOperatorTests {

        @Test
        @DisplayName("FILL_DEFAULT fills null field from fieldDefaults map")
        void fillsDefaultForNullField() throws Exception {
            QualityConfig qc = new QualityConfig();
            qc.setNullStrategy(NullStrategy.FILL_DEFAULT);
            qc.setFieldDefaults(Map.of("countryCode", "XX", "platform", "UNKNOWN"));

            NullHandlerOperator op = new NullHandlerOperator(buildStreamDef(qc));
            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("countryCode", null);
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertEquals("XX", out.collected.get(0).getField("countryCode").toString());
            assertTrue(dlq.collected.isEmpty());
        }

        @Test
        @DisplayName("FORWARD strategy passes null fields unchanged")
        void forwardPassesNulls() throws Exception {
            QualityConfig qc = new QualityConfig();
            qc.setNullStrategy(NullStrategy.FORWARD);

            NullHandlerOperator op = new NullHandlerOperator(buildStreamDef(qc));
            ListCollector<RawStreamEvent> out = new ListCollector<>();
            ListCollector<FailedEvent> dlq = new ListCollector<>();

            GenericRecord rec = record("e1", "c1", 1000L);
            rec.put("email", null);
            RawStreamEvent evt = event(rec);

            op.processElement(evt, new TestProcessContext<>(dlq), out);

            assertEquals(1, out.collected.size());
            assertTrue(dlq.collected.isEmpty());
        }
    }

    // =========================================================================
    // StreamTopologyConfig YAML loading tests
    // =========================================================================

    @Nested
    @DisplayName("StreamTopologyConfig")
    class StreamTopologyConfigTests {

        @Test
        @DisplayName("loads streams-topology.yml with new metadata format")
        void loadsYaml() {
            StreamTopologyConfig topology = StreamTopologyConfig.load();
            assertFalse(topology.getStreams().isEmpty(),
                    "streams-topology.yml should contain at least one stream");
        }

        @Test
        @DisplayName("customer-profile stream has correct source and quality config")
        void customerProfileHasCorrectConfig() {
            StreamTopologyConfig topology = StreamTopologyConfig.load();

            StreamDefinition def = topology.getStreams().stream()
                    .filter(s -> "customer-profile".equals(s.getKey()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("customer-profile stream not found"));

            assertTrue(def.getSource() instanceof KinesisSourceConfig,
                    "customer-profile source should be KINESIS");
            assertEquals(StreamType.CUSTOMER, def.getStreamType());

            QualityConfig qc = def.getQuality();
            assertTrue(qc != null, "quality config should not be null");
            assertEquals("eventId", qc.getDedupKey());
            assertTrue(qc.getRequiredFields().contains("customerId"));
            assertTrue(qc.getSensitiveFields().contains("email"));
        }

        @Test
        @DisplayName("all 19 streams load without error and have ICEBERG sinks")
        void allStreamsLoad() {
            StreamTopologyConfig topology = StreamTopologyConfig.load();
            assertEquals(19, topology.getStreams().size(),
                    "Should have exactly 19 streams");

            for (StreamDefinition def : topology.getStreams()) {
                assertTrue(def.primaryIcebergSink() != null,
                        "Stream '" + def.getKey() + "' should have at least one ICEBERG sink");
                assertTrue(def.getSource() != null,
                        "Stream '" + def.getKey() + "' should have a source config");
                assertTrue(def.getSchema() != null,
                        "Stream '" + def.getKey() + "' should have a schema config");
            }
        }

        @Test
        @DisplayName("DLQ config loads with ICEBERG type")
        void dlqConfigLoads() {
            StreamTopologyConfig topology = StreamTopologyConfig.load();
            assertEquals(StreamTopologyConfig.DlqType.ICEBERG, topology.getDlq().getType());
            assertEquals("system", topology.getDlq().getDatabase());
            assertEquals("dlq_events", topology.getDlq().getTable());
        }

        @Test
        @DisplayName("backward-compat getters return correct values")
        void backwardCompatGetters() {
            StreamTopologyConfig topology = StreamTopologyConfig.load();
            StreamDefinition def = topology.getStreams().stream()
                    .filter(s -> "customer-profile".equals(s.getKey()))
                    .findFirst()
                    .orElseThrow();

            assertEquals("customer-profile.avsc", def.getAvroSchema());
            assertEquals("CustomerProfile", def.getGlueSchemaName());
            assertEquals("customer.customer_profiles", def.getIcebergTable());
            assertFalse(def.getPartitionBy().isEmpty());
            assertTrue(def.getSensitiveFields().contains("email"));
            assertTrue(def.hasSensitiveFields());
        }
    }

    // ── Test utilities ─────────────────────────────────────────────────────────

    /**
     * Minimal process context that routes DLQ side-output to a {@link ListCollector}.
     */
    static class TestProcessContext<T>
            extends org.apache.flink.streaming.api.functions.ProcessFunction<T, T>.Context {

        private final ListCollector<FailedEvent> dlqCollector;

        TestProcessContext(ListCollector<FailedEvent> dlqCollector) {
            // ProcessFunction.Context is abstract — use anonymous subclass
            this.dlqCollector = dlqCollector;
        }

        @Override
        public Long timestamp() { return null; }

        @Override
        public org.apache.flink.streaming.api.TimerService timerService() { return null; }

        @Override
        public <X> void output(org.apache.flink.util.OutputTag<X> outputTag, X value) {
            if (QualityChainBuilder.DLQ_TAG.equals(outputTag)) {
                dlqCollector.collect((FailedEvent) value);
            }
        }
    }
}
