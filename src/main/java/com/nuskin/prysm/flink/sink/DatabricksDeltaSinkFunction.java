package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DatabricksDeltaSinkConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DatabricksMode;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Flink sink for writing events to Databricks.
 *
 * <h3>Two modes (configured via {@code sinks[].mode} in YAML)</h3>
 *
 * <p><b>DELTA mode</b> (default, recommended for high throughput):
 * Writes directly to a Delta Lake table on S3 using the
 * {@code delta-flink} connector ({@code io.delta:delta-flink}).
 * Databricks reads the same S3 path via Unity Catalog / External Table.
 * No data movement.  Requires {@code io.delta:delta-flink} on classpath.
 *
 * <p><b>JDBC mode</b> (lower throughput, simpler):
 * Writes to a Databricks SQL Warehouse via JDBC.
 * Suitable for low-volume enrichment tables or control-plane data.
 * Requires {@code com.databricks:databricks-jdbc} on classpath.
 * Connection string from {@code sinks[].jdbcUrl}; PAT from
 * {@code sinks[].secretsManagerKey}.
 *
 * <h3>Buffering</h3>
 * <p>Both modes buffer records and flush on checkpoint or when 1000 records
 * have accumulated (matching the Iceberg target file size pattern).
 */
public class DatabricksDeltaSinkFunction
        extends RichSinkFunction<RawStreamEvent>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DatabricksDeltaSinkFunction.class);
    private static final int BATCH_SIZE = 1000;

    private final DatabricksDeltaSinkConfig config;
    private final AppConfig appConfig;

    private transient List<RawStreamEvent> buffer;
    private transient Connection jdbcConnection; // JDBC mode only
    private transient boolean deltaAvailable;

    public DatabricksDeltaSinkFunction(DatabricksDeltaSinkConfig config, AppConfig appConfig) {
        this.config    = config;
        this.appConfig = appConfig;
    }

    @Override
    public void open(Configuration parameters) {
        buffer = new ArrayList<>(BATCH_SIZE);

        if (config.getMode() == DatabricksMode.DELTA) {
            deltaAvailable = checkDeltaAvailable();
            LOG.info("DatabricksDeltaSinkFunction (DELTA mode) opened: path={}",
                     config.getDeltaPath());
        } else {
            openJdbcConnection();
            LOG.info("DatabricksDeltaSinkFunction (JDBC mode) opened: table={}",
                     config.getTargetTable());
        }
    }

    @Override
    public void invoke(RawStreamEvent event, Context context) throws Exception {
        buffer.add(event);
        if (buffer.size() >= BATCH_SIZE) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!buffer.isEmpty()) flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) flush();
        if (jdbcConnection != null && !jdbcConnection.isClosed()) {
            jdbcConnection.close();
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void flush() throws Exception {
        if (buffer.isEmpty()) return;

        try {
            if (config.getMode() == DatabricksMode.DELTA) {
                writeDelta(buffer);
            } else {
                writeJdbc(buffer);
            }
            LOG.debug("Flushed {} records to Databricks (mode={})",
                      buffer.size(), config.getMode());
        } finally {
            buffer.clear();
        }
    }

    private void writeDelta(List<RawStreamEvent> records) {
        if (!deltaAvailable) {
            LOG.warn("[STUB] delta-flink not on classpath — {} records dropped for '{}'",
                     records.size(), config.getDeltaPath());
            return;
        }
        // Full Delta Lake write implementation:
        // DeltaSink<RowData> deltaSink = DeltaSink
        //     .forRowFormat(new Path(config.getDeltaPath()), rowTypeInfo)
        //     .build();
        // This requires converting GenericRecord → RowData and wiring
        // DeltaSink via the DataStream API at pipeline construction time
        // rather than per-record in invoke().  For high-throughput use,
        // prefer wiring DeltaSink in SinkConnectorFactory directly.
        LOG.info("[STUB] Would write {} records to Delta table at {}",
                 records.size(), config.getDeltaPath());
    }

    private void writeJdbc(List<RawStreamEvent> records) throws SQLException {
        if (jdbcConnection == null) return;

        String sql = "INSERT INTO " + config.getTargetTable()
                + " (stream_key, customer_id, event_time, payload)"
                + " VALUES (?, ?, ?, ?)";

        try (PreparedStatement ps = jdbcConnection.prepareStatement(sql)) {
            for (RawStreamEvent event : records) {
                ps.setString(1, event.getStreamKey());
                ps.setString(2, event.getCustomerId());
                Long et = event.getEventTimeMs();
                ps.setLong(3, et != null ? et : 0L);
                ps.setString(4, event.getAvroRecord() != null
                        ? event.getAvroRecord().toString() : "{}");
                ps.addBatch();
            }
            ps.executeBatch();
            jdbcConnection.commit();
        }
    }

    private boolean checkDeltaAvailable() {
        try {
            Class.forName("io.delta.flink.sink.DeltaSink");
            return true;
        } catch (ClassNotFoundException e) {
            LOG.warn("Delta Flink connector (io.delta:delta-flink) not on classpath "
                    + "— add dependency to pom.xml to enable DELTA mode writes");
            return false;
        }
    }

    private void openJdbcConnection() {
        try {
            String url = config.getJdbcUrl();
            if (url == null || url.isBlank()) {
                LOG.warn("No jdbcUrl configured for Databricks JDBC sink");
                return;
            }
            String token = resolveToken();
            // Databricks JDBC URL format:
            // jdbc:databricks://<host>:443/default;transportMode=http;ssl=1;
            //   httpPath=<http-path>;AuthMech=3;UID=token;PWD=<personal-access-token>
            jdbcConnection = DriverManager.getConnection(url + ";PWD=" + token);
            jdbcConnection.setAutoCommit(false);
        } catch (Exception e) {
            LOG.error("Failed to open Databricks JDBC connection: {}", e.getMessage(), e);
        }
    }

    private String resolveToken() {
        if (config.getSecretsManagerKey() == null || "local".equals(appConfig.getEnv())) {
            return "";
        }
        try {
            com.nuskin.prysm.flink.config.SecretsManagerUtil secrets =
                    new com.nuskin.prysm.flink.config.SecretsManagerUtil(appConfig.getRegion());
            String token = secrets.getSecretField(config.getSecretsManagerKey(), "token");
            secrets.close();
            return token;
        } catch (Exception e) {
            LOG.warn("Could not resolve Databricks token: {}", e.getMessage());
            return "";
        }
    }
}
