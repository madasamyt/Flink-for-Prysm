package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DatabricksDeltaSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DremioFlightSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.IcebergSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.SinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metadata-driven sink connector factory.
 *
 * <p>Iterates the {@code sinks[]} list declared in each {@link StreamDefinition}
 * and attaches the appropriate Flink sink for each configured target.
 * A single stream can fan out to multiple sinks simultaneously (e.g., Iceberg
 * for the data lake AND Dremio Arrow Flight for real-time query access).
 *
 * <h3>Currently supported sink types</h3>
 * <ul>
 *   <li>{@code ICEBERG} — Apache Iceberg on S3 via Glue/Hive/Nessie catalog</li>
 *   <li>{@code DREMIO_FLIGHT} — Dremio via Apache Arrow Flight gRPC</li>
 *   <li>{@code DATABRICKS_DELTA} — Delta Lake on S3 (direct write) or Databricks
 *       SQL Warehouse via JDBC</li>
 *   <li>{@code REDSHIFT} — stub for Amazon Redshift JDBC / S3 COPY</li>
 *   <li>{@code ELASTICSEARCH} — stub for Elasticsearch / OpenSearch</li>
 * </ul>
 */
public class SinkConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SinkConnectorFactory.class);

    private final AppConfig appConfig;
    private final IcebergSinkBuilder icebergSinkBuilder;

    public SinkConnectorFactory(AppConfig appConfig) {
        this.appConfig         = appConfig;
        this.icebergSinkBuilder = new IcebergSinkBuilder(appConfig);
    }

    /**
     * Attaches all configured sinks for the given stream definition to the
     * provided data stream.  Call once per stream after quality processing.
     *
     * @param stream  quality-filtered + encrypted stream for this logical stream
     * @param def     stream topology definition containing the {@code sinks[]} list
     */
    public void attachAll(DataStream<RawStreamEvent> stream, StreamDefinition def) {
        if (def.getSinks() == null || def.getSinks().isEmpty()) {
            LOG.warn("Stream '{}' has no sinks configured — events will be discarded",
                     def.getKey());
            return;
        }

        for (SinkConfig sinkConfig : def.getSinks()) {
            attach(stream, def, sinkConfig);
        }
    }

    private void attach(DataStream<RawStreamEvent> stream,
                        StreamDefinition def,
                        SinkConfig sinkConfig) {
        switch (sinkConfig.getType()) {

            case ICEBERG:
                IcebergSinkConfig icebergCfg = (IcebergSinkConfig) sinkConfig;
                icebergSinkBuilder.attachSink(stream, def, icebergCfg);
                LOG.info("Attached ICEBERG sink for stream '{}' → {}.{}",
                         def.getKey(), icebergCfg.getDatabase(), icebergCfg.getTable());
                break;

            case DREMIO_FLIGHT:
                DremioFlightSinkConfig dremioCfg = (DremioFlightSinkConfig) sinkConfig;
                stream.addSink(new DremioFlightSinkFunction(dremioCfg, appConfig))
                      .name("sink-dremio-" + def.getKey())
                      .uid("sink-dremio-" + def.getKey());
                LOG.info("Attached DREMIO_FLIGHT sink for stream '{}' → {}",
                         def.getKey(), dremioCfg.getTargetTable());
                break;

            case DATABRICKS_DELTA:
                DatabricksDeltaSinkConfig databricksCfg = (DatabricksDeltaSinkConfig) sinkConfig;
                stream.addSink(new DatabricksDeltaSinkFunction(databricksCfg, appConfig))
                      .name("sink-databricks-" + def.getKey())
                      .uid("sink-databricks-" + def.getKey());
                LOG.info("Attached DATABRICKS_DELTA sink for stream '{}' (mode={})",
                         def.getKey(), databricksCfg.getMode());
                break;

            case REDSHIFT:
                throw new UnsupportedOperationException(
                        "REDSHIFT sink not yet wired — implement RedshiftSinkFunction "
                        + "using flink-connector-jdbc + Redshift JDBC driver");

            case ELASTICSEARCH:
                throw new UnsupportedOperationException(
                        "ELASTICSEARCH sink not yet wired — add "
                        + "flink-connector-elasticsearch7 dependency");

            default:
                throw new IllegalArgumentException(
                        "Unknown sink type '" + sinkConfig.getType()
                        + "' for stream '" + def.getKey() + "'");
        }
    }
}
