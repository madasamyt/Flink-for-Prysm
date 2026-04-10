package com.nuskin.prysm.flink.source;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Metadata-driven source connector factory.
 *
 * <p>Reads the {@code source.type} field from each {@link StreamDefinition} in
 * {@code streams-topology.yml} and dispatches to the appropriate connector
 * implementation.  Adding a new source type requires adding a new case here and
 * a new {@code XxxSourceConfig} subclass — no changes to any job class.
 *
 * <h3>Currently supported source types</h3>
 * <ul>
 *   <li>{@code KINESIS} — AWS Kinesis Enhanced Fan-Out via
 *       {@link KinesisSourceBuilder}</li>
 *   <li>{@code KAFKA} — stub; wires up when Kafka connector is added to
 *       the classpath</li>
 * </ul>
 */
public class SourceConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SourceConnectorFactory.class);

    private final AppConfig appConfig;
    private final StreamTopologyConfig topology;

    public SourceConnectorFactory(AppConfig appConfig, StreamTopologyConfig topology) {
        this.appConfig = appConfig;
        this.topology  = topology;
    }

    /**
     * Builds one source per stream definition and unions them into a single
     * {@link DataStream}{@code <RawStreamEvent>}.
     */
    public DataStream<RawStreamEvent> buildUnionSource(StreamExecutionEnvironment env) {
        List<DataStream<RawStreamEvent>> streams = new ArrayList<>();

        for (StreamDefinition def : topology.getStreams()) {
            DataStream<RawStreamEvent> stream = buildSingleSource(def, env);
            streams.add(stream);
            LOG.info("Registered source for stream '{}' (type={})",
                     def.getKey(),
                     def.getSource() != null ? def.getSource().getType() : "null");
        }

        if (streams.isEmpty()) {
            throw new IllegalStateException("No streams configured in streams-topology.yml");
        }

        DataStream<RawStreamEvent> union = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            union = union.union(streams.get(i));
        }
        return union;
    }

    /**
     * Builds a single source for the given stream definition, dispatching
     * based on {@code source.type} from the YAML configuration.
     */
    public DataStream<RawStreamEvent> buildSingleSource(
            StreamDefinition def,
            StreamExecutionEnvironment env) {

        if (def.getSource() == null) {
            throw new IllegalStateException(
                    "No source config for stream '" + def.getKey() + "'");
        }

        switch (def.getSource().getType()) {
            case KINESIS:
                return new KinesisSourceBuilder(appConfig, topology)
                        .buildSingleSource(env, def);

            case KAFKA:
                throw new UnsupportedOperationException(
                        "KAFKA source not yet wired — add flink-connector-kafka "
                        + "dependency and implement KafkaSourceBuilder");

            case RABBITMQ:
                throw new UnsupportedOperationException(
                        "RABBITMQ source not yet wired — add flink-connector-rabbitmq "
                        + "dependency and implement RabbitMQSourceBuilder");

            case SQS:
                throw new UnsupportedOperationException(
                        "SQS source not yet wired — implement SqsSourceBuilder "
                        + "using AWS SDK v2 SqsAsyncClient");

            case CDC:
                throw new UnsupportedOperationException(
                        "CDC source not yet wired — add flink-cdc-connectors "
                        + "dependency and implement CdcSourceBuilder");

            case FILE:
                throw new UnsupportedOperationException(
                        "FILE source not yet wired — implement FileSourceBuilder "
                        + "using Flink FileSource + S3AFileSystem");

            default:
                throw new IllegalArgumentException(
                        "Unknown source type '" + def.getSource().getType()
                        + "' for stream '" + def.getKey() + "'");
        }
    }
}
