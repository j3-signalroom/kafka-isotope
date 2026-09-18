/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

/**
 * Turns {@link SpanEvent}s into the bytes of an OTLP
 * {@link ExportTraceServiceRequest} — the payload of one record on the spans
 * topic.
 *
 * <p>Emitting the standard OTLP request type (rather than an isotope-shaped
 * envelope) is what removes the need for a bridge on the consuming side: a stock
 * OpenTelemetry Collector with a {@code kafka} receiver set to
 * {@code encoding: otlp_proto} reads this topic directly and exports to whatever
 * backend it is configured with.
 *
 * <p>Spans are grouped by {@code service.name} into one {@link ResourceSpans}
 * each, which is how OTel expects a batch spanning several services to arrive.
 */
final class OtlpSpanEncoder {

    /** Instrumentation scope reported on every span. */
    static final String SCOPE_NAME = "ai.signalroom.kafka.isotope";

    // Semantic-convention attribute keys (messaging.*) plus the isotope-specific
    // ones, which are namespaced so they can't collide with future semconv keys.
    private static final String ATTR_SERVICE_NAME    = "service.name";
    private static final String ATTR_MSG_SYSTEM      = "messaging.system";
    private static final String ATTR_MSG_DESTINATION = "messaging.destination.name";
    private static final String ATTR_MSG_OP_NAME     = "messaging.operation.name";
    private static final String ATTR_MSG_OP_TYPE     = "messaging.operation.type";
    private static final String ATTR_PIPELINE        = "isotope.pipeline";
    private static final String ATTR_ORIGIN_SERVICE  = "isotope.origin_service";
    private static final String ATTR_HOP_COUNT       = "isotope.hop_count";
    private static final String ATTR_TRUNCATED       = "isotope.truncated";

    private static final long NANOS_PER_MS = 1_000_000L;

    private OtlpSpanEncoder() {}

    /**
     * Encodes one batch — in practice the spans of a single trace, since the
     * writer keys records by trace id.
     */
    static byte[] encode(List<SpanEvent> events) {
        return request(events).toByteArray();
    }

    /** The batch as a protobuf message; {@link #encode} is this plus serialization. */
    static ExportTraceServiceRequest request(List<SpanEvent> events) {
        // Preserve arrival order within each service so a reader sees spans in
        // the order the pipeline produced them.
        Map<String, List<SpanEvent>> byService = new LinkedHashMap<>();
        for (SpanEvent e : events) {
            byService.computeIfAbsent(e.service(), k -> new ArrayList<>()).add(e);
        }

        ExportTraceServiceRequest.Builder request = ExportTraceServiceRequest.newBuilder();
        for (Map.Entry<String, List<SpanEvent>> entry : byService.entrySet()) {
            ScopeSpans.Builder scopeSpans = ScopeSpans.newBuilder()
                .setScope(InstrumentationScope.newBuilder().setName(SCOPE_NAME).build());
            for (SpanEvent e : entry.getValue()) {
                scopeSpans.addSpans(span(e));
            }
            request.addResourceSpans(ResourceSpans.newBuilder()
                .setResource(Resource.newBuilder()
                    .addAttributes(stringAttr(ATTR_SERVICE_NAME, entry.getKey()))
                    .build())
                .addScopeSpans(scopeSpans.build())
                .build());
        }
        return request.build();
    }

    private static Span span(SpanEvent e) {
        boolean consume = e.kind() == SpanEvent.Kind.CONSUME;

        Span.Builder span = Span.newBuilder()
            // The isotope trace id is a 16-byte UUIDv7 — exactly OTel's 128-bit
            // trace id, so it carries over with no transformation at all.
            .setTraceId(ByteString.copyFrom(e.traceId()))
            .setSpanId(ByteString.copyFrom(SpanIds.spanId(e)))
            .setName((consume ? "receive " : "send ") + e.topic())
            .setKind(consume ? Span.SpanKind.SPAN_KIND_CONSUMER : Span.SpanKind.SPAN_KIND_PRODUCER)
            .setStartTimeUnixNano(e.startTsMs() * NANOS_PER_MS)
            .setEndTimeUnixNano(e.tsMs() * NANOS_PER_MS)
            .addAttributes(stringAttr(ATTR_MSG_SYSTEM, "kafka"))
            .addAttributes(stringAttr(ATTR_MSG_DESTINATION, e.topic()))
            .addAttributes(stringAttr(ATTR_MSG_OP_NAME, consume ? "receive" : "send"))
            .addAttributes(stringAttr(ATTR_MSG_OP_TYPE, consume ? "receive" : "publish"))
            .addAttributes(stringAttr(ATTR_PIPELINE, e.pipeline()))
            .addAttributes(stringAttr(ATTR_ORIGIN_SERVICE, e.originService()))
            .addAttributes(intAttr(ATTR_HOP_COUNT, e.hopCount()))
            .addAttributes(boolAttr(ATTR_TRUNCATED, e.truncated()));

        byte[] parent = SpanIds.parentSpanId(e);
        if (parent != null) {
            span.setParentSpanId(ByteString.copyFrom(parent));
        }
        return span.build();
    }

    private static KeyValue stringAttr(String key, String value) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value).build())
            .build();
    }

    private static KeyValue intAttr(String key, long value) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setIntValue(value).build())
            .build();
    }

    private static KeyValue boolAttr(String key, boolean value) {
        return KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setBoolValue(value).build())
            .build();
    }
}
