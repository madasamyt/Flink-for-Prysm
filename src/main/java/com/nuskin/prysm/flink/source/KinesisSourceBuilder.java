package com.nuskin.prysm.flink.source;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import com.nuskin.prysm.flink.schema.PrysmAvroDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.enumerator.KinesisShardAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Factory that creates Flink {@link KinesisStreamsSource} instances for each
 * of the 19 Prysm Kinesis streams and unions them into a single
 * {@link DataStream}{@code <RawStreamEvent>}.
 *
 * <h3>Enhanced Fan-Out (EFO)</h3>
 * <p>All consumers use EFO, which gives each consumer its own 2 MB/s read
 * throughput per shard — essential when multiple Flink jobs or Lambda
 * functions consume the same stream concurrently. EFO consumers are
 * registered lazily on first use.
 *
 * <h3>Watermarks</h3>
 * <p>Watermarks are derived from the {@code eventTime} field in each Avro
 * record with a bounded-out-of-orderness strategy (configurable lag).
 * This ensures correct windowing in the analytics and recommendation jobs
 * even when records arrive slightly out of order due to producer retries.
 */
public class KinesisSourceBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisSourceBuilder.class);

    private static final Duration WATERMARK_MAX_OUT_OF_ORDER = Duration.ofSeconds(30);
    private static final Duration WATERMARK_IDLE_TIMEOUT     = Duration.ofMinutes(5);

    private final AppConfig appConfig;
    private final StreamTopologyConfig topology;

    public KinesisSourceBuilder(AppConfig appConfig, StreamTopologyConfig topology) {
        this.appConfig = appConfig;
        this.topology  = topology;
    }

    /**
     * Builds and unions all 19 stream sources into a single tagged stream.
     * Each event in the returned stream carries its originating
     * {@link StreamDefinition} so downstream operators can route it.
     *
     * @param env the Flink execution environment
     * @return merged stream of all 19 Kinesis sources
     */
    public DataStream<RawStreamEvent> buildUnionSource(StreamExecutionEnvironment env) {
        List<DataStream<RawStreamEvent>> streams = new ArrayList<>();

        for (StreamDefinition streamDef : topology.getStreams()) {
            DataStream<RawStreamEvent> stream = buildSingleSource(env, streamDef);
            streams.add(stream);
            LOG.info("Registered Kinesis source for stream '{}'", streamDef.getKey());
        }

        if (streams.isEmpty()) {
            throw new IllegalStateException("No streams configured in streams-topology.yml");
        }

        // Union all 19 sources into one stream for shared pre-processing
        DataStream<RawStreamEvent> union = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            union = union.union(streams.get(i));
        }
        return union;
    }

    /**
     * Builds a {@link KinesisStreamsSource} for a single stream definition.
     * The source is named after the stream key for easy identification in
     * the Flink Web UI.
     */
    public DataStream<RawStreamEvent> buildSingleSource(
            StreamExecutionEnvironment env,
            StreamDefinition streamDef) {

        String streamName = streamDef.getResolvedStreamName();
        if (streamName == null || streamName.isBlank()) {
            throw new IllegalStateException(
                    "Stream name not resolved for key: " + streamDef.getKey()
                    + " — ensure AppConfig.resolveStreamTopology() was called");
        }

        KinesisStreamsSource<RawStreamEvent> source = KinesisStreamsSource
                .<RawStreamEvent>builder()
                .setStreamArn(buildStreamArn(streamName))
                .setDeserializationSchema(
                        new PrysmAvroDeserializationSchema(appConfig, streamDef))
                .setKinesisShardAssigner(KinesisShardAssigner.uniformShardAssigner())
                .setSourceProperties(buildConsumerProperties(streamDef))
                .build();

        WatermarkStrategy<RawStreamEvent> watermarkStrategy = WatermarkStrategy
                .<RawStreamEvent>forBoundedOutOfOrderness(WATERMARK_MAX_OUT_OF_ORDER)
                .withTimestampAssigner((event, ts) -> {
                    Long et = event.getEventTimeMs();
                    return et != null ? et : ts;
                })
                .withIdleness(WATERMARK_IDLE_TIMEOUT);

        return env.fromSource(
                source,
                watermarkStrategy,
                "kinesis-source-" + streamDef.getKey())
                .uid("src-" + streamDef.getKey())
                .name("Kinesis[" + streamDef.getKey() + "]");
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private String buildStreamArn(String streamName) {
        // If already an ARN, return as-is
        if (streamName.startsWith("arn:")) {
            return streamName;
        }
        // Build ARN from region and assumed account (resolved from environment)
        String accountId = System.getenv().getOrDefault("AWS_ACCOUNT_ID", "000000000000");
        return String.format("arn:aws:kinesis:%s:%s:stream/%s",
                appConfig.getRegion(), accountId, streamName);
    }

    private Properties buildConsumerProperties(StreamDefinition streamDef) {
        Properties props = new Properties();

        // Starting position — use TRIM_HORIZON for initial backfill
        String startingPos = System.getenv().getOrDefault(
                "KINESIS_STARTING_POSITION",
                appConfig.getProp("kinesis.consumer.starting.position", "LATEST"));
        props.setProperty("flink.stream.initpos", startingPos);

        // Enhanced Fan-Out consumer name (with env substitution)
        String consumerName = streamDef.getEfoConsumer()
                .replace("{env}", appConfig.getEnv());
        props.setProperty("flink.stream.recordpublisher", "EFO");
        props.setProperty("flink.stream.efo.consumername", consumerName);
        props.setProperty("flink.stream.efo.registration", "LAZY");

        // Deaggregation for KPL-aggregated records
        props.setProperty("flink.stream.kinesis.deaggregation", "true");

        // Region
        props.setProperty("aws.region", appConfig.getRegion());

        return props;
    }
}
