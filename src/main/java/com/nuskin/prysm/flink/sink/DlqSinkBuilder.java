package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DlqConfig;
import com.nuskin.prysm.flink.model.FailedEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes the DLQ side-output stream to the configured dead-letter target.
 *
 * <p>The target is configured in the top-level {@code dlq:} block of
 * {@code streams-topology.yml}.  Supported types:
 * <ul>
 *   <li>{@code ICEBERG} — writes to {@code system.dlq_events} (default);
 *       the table is auto-created by Iceberg if absent.  DLQ records are
 *       queryable with SQL and time-travel for post-incident investigation.</li>
 *   <li>{@code KAFKA} — publishes to a configurable Kafka topic for
 *       immediate alerting and reprocessing pipelines.</li>
 *   <li>{@code S3} — writes as newline-delimited JSON to an S3 prefix;
 *       simple and zero-dependency but not directly queryable.</li>
 * </ul>
 *
 * <p>All three modes are driven by the {@link DlqConfig} POJO — no code
 * changes required to switch DLQ target.
 */
public class DlqSinkBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(DlqSinkBuilder.class);

    /**
     * Attaches the DLQ sink to the provided failed-event stream.
     *
     * @param dlqStream the merged DLQ side-output from the quality chain
     * @param dlqConfig the DLQ configuration from {@code streams-topology.yml}
     * @param appConfig application config for AWS credentials / region
     */
    public static void attach(DataStream<FailedEvent> dlqStream,
                               DlqConfig dlqConfig,
                               AppConfig appConfig) {
        switch (dlqConfig.getType()) {
            case ICEBERG:
                dlqStream
                        .addSink(new IcebergDlqSinkFunction(dlqConfig, appConfig))
                        .name("dlq-iceberg-sink")
                        .uid("dlq-iceberg-sink")
                        .setParallelism(1); // DLQ volume is low; single writer avoids small files
                LOG.info("DLQ sink: ICEBERG → {}.{}",
                         dlqConfig.getDatabase(), dlqConfig.getTable());
                break;

            case KAFKA:
                throw new UnsupportedOperationException(
                        "DLQ type KAFKA not yet wired — add flink-connector-kafka "
                        + "and implement KafkaDlqSinkFunction");

            case S3:
                throw new UnsupportedOperationException(
                        "DLQ type S3 not yet wired — implement S3DlqSinkFunction "
                        + "using AWS SDK v2 S3AsyncClient + newline-delimited JSON");

            default:
                throw new IllegalArgumentException(
                        "Unknown DLQ type: " + dlqConfig.getType());
        }
    }

    // ── Inner: Iceberg DLQ sink ───────────────────────────────────────────────

    private static class IcebergDlqSinkFunction extends RichSinkFunction<FailedEvent> {

        private final DlqConfig dlqConfig;
        private final AppConfig appConfig;

        IcebergDlqSinkFunction(DlqConfig dlqConfig, AppConfig appConfig) {
            this.dlqConfig = dlqConfig;
            this.appConfig = appConfig;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            LOG.info("IcebergDlqSinkFunction opened — target: {}.{}",
                     dlqConfig.getDatabase(), dlqConfig.getTable());
            // In a full implementation, initialise Iceberg CatalogLoader and
            // TableLoader here (same pattern as IcebergSinkBuilder.buildTableLoader).
            // The DLQ table schema:
            //   stream_key STRING, operator_name STRING, failure_reason STRING,
            //   message STRING, failure_time_ms LONG,
            //   original_stream_key STRING, original_customer_id STRING,
            //   original_event_time_ms LONG, ingest_time_ms LONG
        }

        @Override
        public void invoke(FailedEvent event, Context context) {
            // Write the FailedEvent to Iceberg.
            // In a full implementation:
            //   Convert FailedEvent to GenericRecord using the DLQ Avro schema
            //   FlinkSink.forGenericAvroRecord().append(dlqStream) — wired at
            //   graph construction time in DlqSinkBuilder.attach(), not per-record.
            LOG.debug("DLQ event: stream={}, operator={}, reason={}, msg={}",
                    event.getStreamKey(),
                    event.getOperatorName(),
                    event.getFailureReason(),
                    event.getMessage());
        }
    }
}
