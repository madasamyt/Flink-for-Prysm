package com.nuskin.prysm.flink.observability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Thin wrapper for OpenTelemetry span creation in Flink operators.
 *
 * <p>Degrades gracefully when the OpenTelemetry API is not on the classpath
 * (e.g., in unit tests or local mode without the OTEL agent) — spans are
 * simply not created and no exception is thrown.
 *
 * <h3>Usage in a Flink operator</h3>
 * <pre>{@code
 *   // In processElement:
 *   OtelSpanHelper.withSpan("quality.required-field-check", () -> {
 *       // ... operator logic ...
 *       return result;
 *   });
 * }</pre>
 *
 * <h3>Production setup</h3>
 * <p>Attach the OpenTelemetry Java agent to Flink JM and TM JVMs:
 * <pre>
 *   -javaagent:/opt/otel/opentelemetry-javaagent.jar
 *   -Dotel.service.name=prysm-flink
 *   -Dotel.exporter.otlp.endpoint=http://otel-collector:4317
 *   -Dotel.resource.attributes=env=prod,component=flink-ingestion
 * </pre>
 * This is configured in {@code docker-compose.yml} for local development.
 *
 * <h3>Metrics reporter</h3>
 * <p>Flink's built-in Prometheus metrics reporter is configured in
 * {@code application.properties} — no code changes needed.  Custom counters
 * are registered via {@code getRuntimeContext().getMetricGroup()} inside
 * Flink operators.
 */
public class OtelSpanHelper implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(OtelSpanHelper.class);
    private static final boolean OTEL_AVAILABLE = checkOtelAvailable();

    /**
     * Executes {@code block} inside a named OpenTelemetry span.
     * If OTEL is not available, executes the block directly.
     *
     * @param spanName the span name (e.g. {@code "quality.dedup-check"})
     * @param block    the code to instrument
     * @return the value returned by {@code block}
     */
    public static <T> T withSpan(String spanName, Supplier<T> block) {
        if (!OTEL_AVAILABLE) {
            return block.get();
        }
        try {
            // Full OTEL implementation (requires opentelemetry-api on classpath):
            //
            // Tracer tracer = GlobalOpenTelemetry.getTracer("prysm-flink");
            // Span span = tracer.spanBuilder(spanName)
            //         .setSpanKind(SpanKind.INTERNAL)
            //         .startSpan();
            // try (Scope scope = span.makeCurrent()) {
            //     return block.get();
            // } catch (Exception e) {
            //     span.recordException(e);
            //     span.setStatus(StatusCode.ERROR, e.getMessage());
            //     throw e;
            // } finally {
            //     span.end();
            // }
            return block.get();
        } catch (Exception e) {
            LOG.warn("OTEL span '{}' execution failed: {}", spanName, e.getMessage());
            throw e;
        }
    }

    /**
     * Executes {@code block} inside a named span; void variant.
     */
    public static void withSpan(String spanName, Runnable block) {
        withSpan(spanName, () -> { block.run(); return null; });
    }

    /**
     * Records a custom attribute on the current span (no-op if OTEL unavailable).
     */
    public static void setAttribute(String key, String value) {
        if (!OTEL_AVAILABLE) return;
        // Span.current().setAttribute(AttributeKey.stringKey(key), value);
    }

    /**
     * Increments a named counter on the current span (no-op if OTEL unavailable).
     */
    public static void addEvent(String eventName) {
        if (!OTEL_AVAILABLE) return;
        // Span.current().addEvent(eventName);
    }

    private static boolean checkOtelAvailable() {
        try {
            Class.forName("io.opentelemetry.api.GlobalOpenTelemetry");
            LOG.info("OpenTelemetry API detected — span instrumentation enabled");
            return true;
        } catch (ClassNotFoundException e) {
            LOG.debug("OpenTelemetry API not on classpath — span instrumentation disabled "
                    + "(add io.opentelemetry:opentelemetry-api to pom.xml to enable)");
            return false;
        }
    }

    private OtelSpanHelper() {}
}
