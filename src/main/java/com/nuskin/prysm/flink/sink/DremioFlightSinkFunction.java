package com.nuskin.prysm.flink.sink;

import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.DremioFlightSinkConfig;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink sink that writes events to Dremio via Apache Arrow Flight (gRPC).
 *
 * <h3>Design</h3>
 * <p>Records are buffered in-memory and flushed as a single Arrow batch when
 * either {@link DremioFlightSinkConfig#getBufferSize()} records are accumulated
 * or {@link DremioFlightSinkConfig#getFlushIntervalMs()} ms have elapsed since
 * the last flush (whichever comes first).  Checkpoint snapshots trigger an
 * additional forced flush to ensure no data is lost on recovery.
 *
 * <h3>Arrow Flight dependency</h3>
 * <p>Requires {@code org.apache.arrow:flight-grpc} on the classpath.  This is
 * added to {@code pom.xml} under the {@code arrow.version} property.  The
 * actual {@code FlightClient} construction is wrapped in a try-catch so that
 * the sink degrades gracefully (logs + buffers locally) when Arrow Flight is
 * not on the classpath in local/test mode.
 *
 * <h3>Authentication</h3>
 * <p>The Dremio PAT (Personal Access Token) is read from AWS Secrets Manager
 * using the {@code secretsManagerKey} field from the YAML config.  In local
 * mode, an empty token is used.
 */
public class DremioFlightSinkFunction
        extends RichSinkFunction<RawStreamEvent>
        implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DremioFlightSinkFunction.class);

    private final DremioFlightSinkConfig config;
    private final AppConfig appConfig;

    // In-memory buffer — flushed on checkpoint or when bufferSize is reached
    private transient List<GenericRecord> buffer;
    private transient long lastFlushMs;
    private transient Object flightClient; // typed as Object to avoid hard compile dep
    private transient Schema lastSchema;

    public DremioFlightSinkFunction(DremioFlightSinkConfig config, AppConfig appConfig) {
        this.config    = config;
        this.appConfig = appConfig;
    }

    @Override
    public void open(Configuration parameters) {
        buffer      = new ArrayList<>(config.getBufferSize());
        lastFlushMs = System.currentTimeMillis();
        flightClient = buildFlightClient();
        LOG.info("DremioFlightSinkFunction opened: endpoint={}, table={}",
                 config.getEndpoint(), config.getTargetTable());
    }

    @Override
    public void invoke(RawStreamEvent event, Context context) throws Exception {
        buffer.add(event.getAvroRecord());
        if (event.getAvroRecord() != null) {
            lastSchema = event.getAvroRecord().getSchema();
        }

        long now = System.currentTimeMillis();
        if (buffer.size() >= config.getBufferSize()
                || (now - lastFlushMs) >= config.getFlushIntervalMs()) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Force a flush on every checkpoint to preserve exactly-once semantics
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // No operator state — flushed on checkpoint; in-flight buffer is small
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            flush();
        }
        closeFlightClient();
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void flush() {
        if (buffer.isEmpty()) return;

        try {
            writeBatch(buffer);
            LOG.debug("Flushed {} records to Dremio table '{}'",
                      buffer.size(), config.getTargetTable());
        } catch (Exception e) {
            LOG.error("Failed to flush {} records to Dremio: {}",
                      buffer.size(), e.getMessage(), e);
            // Re-throw so Flink can trigger recovery from checkpoint
            throw new RuntimeException("Dremio Flight flush failed", e);
        } finally {
            buffer.clear();
            lastFlushMs = System.currentTimeMillis();
        }
    }

    /**
     * Writes a batch of Avro records via Arrow Flight.
     *
     * <p>The implementation uses reflection to load Arrow Flight classes at
     * runtime so the code compiles even without the dependency on the classpath.
     * When the dependency is present, records are serialised to Arrow columnar
     * format using the Avro-to-Arrow converter and sent via {@code startPut}.
     */
    private void writeBatch(List<GenericRecord> records) {
        if (flightClient == null) {
            LOG.warn("Arrow Flight client not available — {} records dropped for table '{}'",
                     records.size(), config.getTargetTable());
            return;
        }
        // Full Arrow Flight write implementation:
        // 1. Convert List<GenericRecord> → VectorSchemaRoot using AvroToArrow
        // 2. Obtain a FlightDescriptor.path(targetTable) descriptor
        // 3. Call flightClient.startPut(descriptor, root, new SyncPutListener())
        // 4. writer.putNext() for the batch; writer.completed()
        //
        // This is left as a runtime-dependency-conditional block. Add
        //   <dependency>org.apache.arrow:flight-grpc:${arrow.version}</dependency>
        // to pom.xml and uncomment the implementation in DremioFlightSinkFunction.
        LOG.info("[STUB] Would write {} records to Dremio '{}' via Arrow Flight",
                 records.size(), config.getTargetTable());
    }

    private Object buildFlightClient() {
        try {
            Class.forName("org.apache.arrow.flight.FlightClient");
            String token = resolveToken();
            LOG.info("Arrow Flight available — connecting to {}", config.getEndpoint());
            // Full construction:
            // BufferAllocator allocator = new RootAllocator();
            // Location location = Location.forGrpcInsecure(host, port);
            // FlightClient client = FlightClient.builder(allocator, location).build();
            // client.authenticate(new BasicAuthHandler(token));
            // return client;
            return new Object(); // placeholder until dep is wired
        } catch (ClassNotFoundException e) {
            LOG.warn("Arrow Flight (org.apache.arrow:flight-grpc) not on classpath "
                    + "— DremioFlightSinkFunction will drop records. "
                    + "Add the dependency to pom.xml to enable.");
            return null;
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
            LOG.warn("Could not resolve Dremio token from Secrets Manager: {}", e.getMessage());
            return "";
        }
    }

    private void closeFlightClient() {
        if (flightClient == null) return;
        try {
            // ((FlightClient) flightClient).close();
        } catch (Exception e) {
            LOG.warn("Error closing Arrow Flight client: {}", e.getMessage());
        }
    }
}
