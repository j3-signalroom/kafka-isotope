/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.Span;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import ai.signalroom.kafka.isotope.Isotope;

/**
 * Unit tests for the OTLP encoding and the derived span ids — above all that a
 * child's parent id matches the id its parent computes for itself, since that
 * identity is the whole reason hops can link into a trace tree without carrying
 * span ids on the wire.
 */
class OtlpSpanEncoderTest {

    private static final long ORIGIN_TS = 1_700_000_000_000L;
    private static final String PIPELINE = "orders";
    private static final String ORIGIN_SERVICE = "order-intake-service";

    /** A fresh 16-byte UUIDv7 trace id. */
    private static byte[] newTraceId() {
        return Isotope.newTrace(ORIGIN_SERVICE, PIPELINE, ORIGIN_TS).traceId();
    }

    /** A trace with {@code hops} appended. */
    private static Isotope traceWith(Isotope.Hop... hops) {
        Isotope iso = Isotope.newTrace(ORIGIN_SERVICE, PIPELINE, ORIGIN_TS);
        for (Isotope.Hop hop : hops) {
            iso.appendHop(hop);
        }
        return iso;
    }

    private static SpanEvent produceEvent(Isotope iso) {
        List<Isotope.Hop> hops = iso.hops();
        int n = hops.size();
        Isotope.Hop last = hops.get(n - 1);
        return SpanEvent.of(iso, last.service(), last.topic(), last.tsMs(),
            n >= 2 ? hops.get(n - 2) : null, n, SpanEvent.Kind.PRODUCE);
    }

    private static Map<String, KeyValue> attrs(Span span) {
        Map<String, KeyValue> byKey = new HashMap<>();
        for (KeyValue kv : span.getAttributesList()) {
            byKey.put(kv.getKey(), kv);
        }
        return byKey;
    }

    private static List<Span> spansOf(ExportTraceServiceRequest request) {
        return request.getResourceSpansList().stream()
            .flatMap(rs -> rs.getScopeSpansList().stream())
            .flatMap(ss -> ss.getSpansList().stream())
            .toList();
    }

    // ------------------------------------------------------------------
    // Derived span ids
    // ------------------------------------------------------------------

    @Test
    void spanIdsAreDeterministicAndFieldSensitive() {
        byte[] traceId = newTraceId();

        assertArrayEquals(
            SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS),
            SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS),
            "same inputs must give the same id, in any process");
        assertEquals(8, SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS).length);

        assertFalse(java.util.Arrays.equals(
            SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS),
            SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS + 1)),
            "a different hop timestamp is a different span");
        assertFalse(java.util.Arrays.equals(
            SpanIds.spanId(traceId, "svc", "topic-a", ORIGIN_TS),
            SpanIds.spanId(traceId, "svc", "topic-b", ORIGIN_TS)),
            "a different topic is a different span");
        assertFalse(java.util.Arrays.equals(
            SpanIds.spanId(traceId, "svc-a", "topic", ORIGIN_TS),
            SpanIds.spanId(traceId, "svc-b", "topic", ORIGIN_TS)),
            "a different service is a different span");
        assertFalse(java.util.Arrays.equals(
            SpanIds.spanId(newTraceId(), "svc", "topic", ORIGIN_TS),
            SpanIds.spanId(newTraceId(), "svc", "topic", ORIGIN_TS)),
            "the same hop on two different traces is two different spans");
    }

    @Test
    void aChildsParentIdIsTheIdItsParentComputesForItself() {
        Isotope.Hop first = new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 5);
        Isotope.Hop second = new Isotope.Hop("order-enrichment-service", "topic-b", ORIGIN_TS + 25);

        // One trace advancing hop by hop, as the interceptor would drive it: the
        // parent's event is captured before the second hop is ever appended.
        Isotope iso = traceWith(first);
        SpanEvent parentEvent = produceEvent(iso);

        iso.appendHop(second);
        SpanEvent childEvent = produceEvent(iso);

        assertArrayEquals(SpanIds.spanId(parentEvent), SpanIds.parentSpanId(childEvent),
            "the child derives its parent's id from the previous entry in h[]");
    }

    @Test
    void anOriginProduceHasNoParent() {
        SpanEvent origin = produceEvent(traceWith(new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS)));
        org.junit.jupiter.api.Assertions.assertNull(SpanIds.parentSpanId(origin),
            "the first hop is a root span");
    }

    // ------------------------------------------------------------------
    // OTLP payload
    // ------------------------------------------------------------------

    @Test
    void encodesAProduceSpanWithTheIsotopeTraceIdVerbatim() throws InvalidProtocolBufferException {
        Isotope iso = traceWith(new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 40));
        SpanEvent event = produceEvent(iso);

        ExportTraceServiceRequest request =
            ExportTraceServiceRequest.parseFrom(OtlpSpanEncoder.encode(List.of(event)));

        List<Span> spans = spansOf(request);
        assertEquals(1, spans.size());
        Span span = spans.get(0);

        assertArrayEquals(iso.traceId(), span.getTraceId().toByteArray(),
            "the UUIDv7 is already a 128-bit OTel trace id");
        assertEquals(16, span.getTraceId().size());
        assertEquals(8, span.getSpanId().size());
        assertTrue(span.getParentSpanId().isEmpty(), "origin produce is a root span");
        assertEquals("send topic-a", span.getName());
        assertEquals(Span.SpanKind.SPAN_KIND_PRODUCER, span.getKind());

        // No predecessor, so the span runs from the trace origin to the hop.
        assertEquals(ORIGIN_TS * 1_000_000L, span.getStartTimeUnixNano());
        assertEquals((ORIGIN_TS + 40) * 1_000_000L, span.getEndTimeUnixNano());

        Map<String, KeyValue> a = attrs(span);
        assertEquals("kafka", a.get("messaging.system").getValue().getStringValue());
        assertEquals("topic-a", a.get("messaging.destination.name").getValue().getStringValue());
        assertEquals("send", a.get("messaging.operation.name").getValue().getStringValue());
        assertEquals("publish", a.get("messaging.operation.type").getValue().getStringValue());
        assertEquals(PIPELINE, a.get("isotope.pipeline").getValue().getStringValue());
        assertEquals(ORIGIN_SERVICE, a.get("isotope.origin_service").getValue().getStringValue());
        assertEquals(1L, a.get("isotope.hop_count").getValue().getIntValue());
        assertFalse(a.get("isotope.truncated").getValue().getBoolValue());

        Resource resource = request.getResourceSpans(0).getResource();
        assertEquals("service.name", resource.getAttributes(0).getKey());
        assertEquals(ORIGIN_SERVICE, resource.getAttributes(0).getValue().getStringValue());
        assertEquals(OtlpSpanEncoder.SCOPE_NAME,
            request.getResourceSpans(0).getScopeSpans(0).getScope().getName());
    }

    @Test
    void aHopSpanRunsFromThePreviousHopToThisOne() throws InvalidProtocolBufferException {
        Isotope.Hop first = new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 10);
        Isotope.Hop second = new Isotope.Hop("order-enrichment-service", "topic-b", ORIGIN_TS + 90);

        SpanEvent event = produceEvent(traceWith(first, second));
        Span span = spansOf(ExportTraceServiceRequest.parseFrom(
            OtlpSpanEncoder.encode(List.of(event)))).get(0);

        assertEquals((ORIGIN_TS + 10) * 1_000_000L, span.getStartTimeUnixNano(),
            "start is the previous hop, so the span covers the real gap");
        assertEquals((ORIGIN_TS + 90) * 1_000_000L, span.getEndTimeUnixNano());
        assertArrayEquals(SpanIds.spanId(event.traceId(), ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 10),
            span.getParentSpanId().toByteArray());
    }

    @Test
    void aConsumeSpanIsParentedToTheProduceItAnswers() throws InvalidProtocolBufferException {
        Isotope.Hop produced = new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 10);
        Isotope iso = traceWith(produced);

        SpanEvent produceEvent = produceEvent(iso);
        SpanEvent consumeEvent = SpanEvent.of(iso, "shipping-notification-service", "topic-a",
            ORIGIN_TS + 60, produced, 1, SpanEvent.Kind.CONSUME);

        ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(
            OtlpSpanEncoder.encode(List.of(produceEvent, consumeEvent)));

        // Two services, so two resources — this is how a multi-service batch is
        // meant to arrive at a Collector.
        assertEquals(2, request.getResourceSpansCount());
        Span consume = spansOf(request).stream()
            .filter(s -> s.getKind() == Span.SpanKind.SPAN_KIND_CONSUMER)
            .findFirst()
            .orElseThrow();
        Span produce = spansOf(request).stream()
            .filter(s -> s.getKind() == Span.SpanKind.SPAN_KIND_PRODUCER)
            .findFirst()
            .orElseThrow();

        assertEquals("receive topic-a", consume.getName());
        assertEquals("receive", attrs(consume).get("messaging.operation.name")
            .getValue().getStringValue());
        assertArrayEquals(produce.getSpanId().toByteArray(), consume.getParentSpanId().toByteArray(),
            "the consume span hangs off the produce span");
        assertNotEquals(produce.getSpanId(), consume.getSpanId());
        assertEquals((ORIGIN_TS + 10) * 1_000_000L, consume.getStartTimeUnixNano());
        assertEquals((ORIGIN_TS + 60) * 1_000_000L, consume.getEndTimeUnixNano());

        List<ResourceSpans> resources = request.getResourceSpansList();
        assertEquals(ORIGIN_SERVICE,
            resources.get(0).getResource().getAttributes(0).getValue().getStringValue());
        assertEquals("shipping-notification-service",
            resources.get(1).getResource().getAttributes(0).getValue().getStringValue());
    }

    @Test
    void clockSkewCannotProduceANegativeDuration() throws InvalidProtocolBufferException {
        // The previous hop's clock ran ahead of this one's — different machines.
        Isotope.Hop ahead = new Isotope.Hop(ORIGIN_SERVICE, "topic-a", ORIGIN_TS + 500);
        Isotope.Hop behind = new Isotope.Hop("order-enrichment-service", "topic-b", ORIGIN_TS + 100);

        Span span = spansOf(ExportTraceServiceRequest.parseFrom(
            OtlpSpanEncoder.encode(List.of(produceEvent(traceWith(ahead, behind)))))).get(0);

        assertEquals(span.getEndTimeUnixNano(), span.getStartTimeUnixNano(),
            "clamped to a zero-length span rather than a backwards one");
    }

    @Test
    void truncationIsReported() throws InvalidProtocolBufferException {
        Isotope iso = Isotope.newTrace(ORIGIN_SERVICE, PIPELINE, ORIGIN_TS);
        for (int i = 0; i <= Isotope.MAX_HOPS; i++) {
            iso.appendHop(new Isotope.Hop("svc-" + i, "topic-" + i, ORIGIN_TS + i));
        }
        assertTrue(iso.truncated(), "the hop ring has evicted its oldest entry");

        Span span = spansOf(ExportTraceServiceRequest.parseFrom(
            OtlpSpanEncoder.encode(List.of(produceEvent(iso))))).get(0);

        assertTrue(attrs(span).get("isotope.truncated").getValue().getBoolValue());
        assertEquals(Isotope.MAX_HOPS, attrs(span).get("isotope.hop_count").getValue().getIntValue());
    }
}
