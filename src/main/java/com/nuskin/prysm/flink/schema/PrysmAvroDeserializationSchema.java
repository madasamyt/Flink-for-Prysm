package com.nuskin.prysm.flink.schema;

import com.amazonaws.services.schemaregistry.flink.avro.GlueSchemaRegistryAvroDeserializationSchema;
import com.nuskin.prysm.flink.config.AppConfig;
import com.nuskin.prysm.flink.config.StreamTopologyConfig.StreamDefinition;
import com.nuskin.prysm.flink.model.RawStreamEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Deserialises raw Kinesis bytes into {@link RawStreamEvent} objects.
 *
 * <p>Each event is wrapped in a {@code RawStreamEvent} envelope that carries:
 * <ul>
 *   <li>The Avro {@link GenericRecord} (schema-aware, handles drift)</li>
 *   <li>The originating stream key (for routing to the correct Iceberg table)</li>
 *   <li>The {@link StreamDefinition} (PII field list, table name, etc.)</li>
 * </ul>
 *
 * <p>Using {@code GenericRecord} rather than code-generated {@code SpecificRecord}
 * means the pipeline keeps running even after new optional fields are added to a
 * schema — the GenericRecord simply carries those fields along transparently.
 * Code-generated classes are available for downstream jobs that need typed access.
 */
public class PrysmAvroDeserializationSchema
        implements DeserializationSchema<RawStreamEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(PrysmAvroDeserializationSchema.class);

    private final StreamDefinition streamDef;
    private final Map<String, Object> glueConfig;

    private transient GlueSchemaRegistryAvroDeserializationSchema<GenericRecord> delegate;

    public PrysmAvroDeserializationSchema(AppConfig appConfig, StreamDefinition streamDef) {
        this.streamDef = streamDef;
        this.glueConfig = GlueSchemaRegistryConfig.forStream(appConfig, streamDef);
    }

    @Override
    public void open(InitializationContext context) {
        delegate = GlueSchemaRegistryAvroDeserializationSchema
                .forGeneric(null, glueConfig);
    }

    @Override
    public RawStreamEvent deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            GenericRecord record = delegate.deserialize(message);
            return new RawStreamEvent(streamDef.getKey(), streamDef, record);
        } catch (Exception e) {
            LOG.warn("Failed to deserialise record from stream '{}', skipping. Error: {}",
                     streamDef.getKey(), e.getMessage());
            // Return null — Flink's source will skip nulls
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(RawStreamEvent nextElement) {
        // Kinesis streams are unbounded
        return false;
    }

    @Override
    public TypeInformation<RawStreamEvent> getProducedType() {
        return Types.POJO(RawStreamEvent.class);
    }
}
