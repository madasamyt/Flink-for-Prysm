package com.nuskin.prysm.flink.transform;

import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamType;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.model.SkinHealthSnapshot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for {@link ScanMetricsAggregator} using a local mini Flink cluster.
 *
 * <p>Tests that events from multiple scan dimension streams are correctly merged
 * into a {@link SkinHealthSnapshot} when the {@code scan-overall} event arrives.
 */
class ScanMetricsAggregatorTest {

    @Test
    void aggregatorEmitsSnapshotOnOverallEvent() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setParallelism(1);

        String scanId = UUID.randomUUID().toString();
        String customerId = "cust-001";

        List<RawStreamEvent> events = new ArrayList<>();
        events.add(makeEvent("scan-hydration",    scanId, customerId, "overallHydrationScore",   72.5f));
        events.add(makeEvent("scan-pigmentation", scanId, customerId, "overallPigmentationScore", 38.0f));
        events.add(makeEvent("scan-texture",      scanId, customerId, "overallTextureScore",      61.0f));
        events.add(makeEvent("scan-wrinkles",     scanId, customerId, "overallWrinkleScore",      45.0f));
        events.add(makeEvent("scan-radiance",     scanId, customerId, "radianceScore",            80.0f));
        events.add(makeOverallEvent(scanId, customerId, 68.5f));

        List<SkinHealthSnapshot> collected = new ArrayList<>();

        DataStream<SkinHealthSnapshot> output = env.fromCollection(events)
                .keyBy(e -> {
                    Object id = e.getField("scanId");
                    return id != null ? id.toString() : e.getCustomerId();
                })
                .process(new ScanMetricsAggregator());

        // Collect results via sink
        output.executeAndCollect().forEachRemaining(collected::add);

        assertEquals(1, collected.size(), "Expected exactly one snapshot for the scan");
        SkinHealthSnapshot snap = collected.get(0);
        assertEquals(customerId, snap.getCustomerId());
        assertEquals(scanId, snap.getScanId());
        assertNotNull(snap.getScore("hydration"));
        assertEquals(72.5, snap.getScore("hydration"), 0.01);
        assertEquals(38.0, snap.getScore("pigmentation"), 0.01);
        assertEquals(68.5, snap.getScore("overall"), 0.01);
    }

    @Test
    void aggregatorHandlesMissingDimension() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        env.setParallelism(1);

        String scanId = UUID.randomUUID().toString();
        String customerId = "cust-002";

        // Only hydration + overall (no wrinkles, texture, etc.)
        List<RawStreamEvent> events = new ArrayList<>();
        events.add(makeEvent("scan-hydration", scanId, customerId, "overallHydrationScore", 55.0f));
        events.add(makeOverallEvent(scanId, customerId, 55.0f));

        List<SkinHealthSnapshot> collected = new ArrayList<>();
        env.fromCollection(events)
                .keyBy(e -> {
                    Object id = e.getField("scanId");
                    return id != null ? id.toString() : e.getCustomerId();
                })
                .process(new ScanMetricsAggregator())
                .executeAndCollect()
                .forEachRemaining(collected::add);

        assertEquals(1, collected.size());
        assertNotNull(collected.get(0).getScore("hydration"));
        assertNull(collected.get(0).getScore("wrinkle"),
                   "Wrinkle score should be null when dimension was not received");
    }

    // ── Test data builders ────────────────────────────────────────────────────

    private RawStreamEvent makeEvent(String streamKey, String scanId, String customerId,
                                      String scoreField, float scoreValue) {
        Schema schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"Test\",\"fields\":["
                + "{\"name\":\"customerId\",\"type\":\"string\"},"
                + "{\"name\":\"scanId\",\"type\":\"string\"},"
                + "{\"name\":\"eventTime\",\"type\":\"long\"},"
                + "{\"name\":\"" + scoreField + "\",\"type\":[\"null\",\"float\"],\"default\":null}"
                + "]}");

        GenericRecord rec = new GenericData.Record(schema);
        rec.put("customerId", customerId);
        rec.put("scanId", scanId);
        rec.put("eventTime", System.currentTimeMillis());
        rec.put(scoreField, scoreValue);

        StreamDefinition def = makeStreamDef(streamKey);
        return new RawStreamEvent(streamKey, def, rec);
    }

    private RawStreamEvent makeOverallEvent(String scanId, String customerId, float overallScore) {
        Schema schema = new Schema.Parser().parse(
                "{\"type\":\"record\",\"name\":\"Overall\",\"fields\":["
                + "{\"name\":\"customerId\",\"type\":\"string\"},"
                + "{\"name\":\"scanId\",\"type\":\"string\"},"
                + "{\"name\":\"eventTime\",\"type\":\"long\"},"
                + "{\"name\":\"overallSkinScore\",\"type\":[\"null\",\"float\"],\"default\":null},"
                + "{\"name\":\"topConcerns\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]}"
                + "]}");

        GenericRecord rec = new GenericData.Record(schema);
        rec.put("customerId", customerId);
        rec.put("scanId", scanId);
        rec.put("eventTime", System.currentTimeMillis());
        rec.put("overallSkinScore", overallScore);
        rec.put("topConcerns", Collections.singletonList("hydration"));

        StreamDefinition def = makeStreamDef("scan-overall");
        return new RawStreamEvent("scan-overall", def, rec);
    }

    private StreamDefinition makeStreamDef(String key) {
        StreamDefinition def = new StreamDefinition();
        def.setKey(key);
        def.setStreamType(StreamType.SCAN);
        def.setSensitiveFields(Collections.emptyList());
        def.setIcebergTable("scan.test_table");
        def.setResolvedStreamName(key);
        return def;
    }
}
