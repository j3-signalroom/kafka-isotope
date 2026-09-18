/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import ai.signalroom.kafka.isotope.Isotope;

/**
 * One pipeline edge, flattened into the few values a span needs, as handed from
 * the producing thread to the span writer's background thread.
 *
 * <p>This exists so the hot path stays trivial: {@code onSend} copies field
 * references into this record and drops it on a bounded queue. Hashing span ids,
 * building protobuf and talking to Kafka all happen later, on the writer thread.
 * It deliberately holds no reference to the mutable {@link Isotope} — that
 * object keeps accumulating hops after the send returns, and a span must
 * describe the edge as it was.
 *
 * <p>{@code parentService} is {@code null} when this edge has no predecessor
 * (the origin produce), in which case {@link #parentTsMs()} is {@code -1}.
 *
 * @param traceId       the trace's 16 bytes — also the OTel trace id, verbatim
 * @param pipeline      the trace's pipeline (origin-set, forwarded)
 * @param originService the service that originated the trace
 * @param originTsMs    trace origin timestamp; the start bound for an origin edge
 * @param service       the service on this edge (producer, or consumer for a consume edge)
 * @param topic         the topic produced to, or consumed from
 * @param tsMs          when this edge happened — the span's end
 * @param parentService the predecessor hop's service, or {@code null} if none
 * @param parentTopic   the predecessor hop's topic, or {@code null} if none
 * @param parentTsMs    the predecessor hop's timestamp — the span's start — or {@code -1}
 * @param hopCount      hops accumulated on the trace at this edge
 * @param truncated     whether the hop ring had already evicted its oldest entry
 * @param kind          produce edge or consume edge
 */
record SpanEvent(
        byte[] traceId,
        String pipeline,
        String originService,
        long originTsMs,
        String service,
        String topic,
        long tsMs,
        String parentService,
        String parentTopic,
        long parentTsMs,
        int hopCount,
        boolean truncated,
        Kind kind) {

    /** Which side of the pipeline an edge describes. */
    enum Kind { PRODUCE, CONSUME }

    /** Builds an event from the trace plus the predecessor hop, if any. */
    static SpanEvent of(Isotope isotope, String service, String topic, long tsMs,
            Isotope.Hop parent, int hopCount, Kind kind) {
        return new SpanEvent(
            isotope.traceId(),
            orUnknown(isotope.pipeline()),
            orUnknown(isotope.originService()),
            isotope.originTsMs(),
            orUnknown(service),
            orUnknown(topic),
            tsMs,
            parent == null ? null : orUnknown(parent.service()),
            parent == null ? null : orUnknown(parent.topic()),
            parent == null ? -1L : parent.tsMs(),
            hopCount,
            isotope.truncated(),
            kind);
    }

    private static String orUnknown(String s) {
        return (s == null || s.isEmpty()) ? "unknown" : s;
    }

    /**
     * The span's start: the predecessor hop's timestamp, so each span covers the
     * real gap between two edges rather than collapsing to a point. An edge with
     * no predecessor starts at the trace origin. Clamped to never exceed
     * {@link #tsMs()}, because the two timestamps come from different machines
     * and a backwards clock would otherwise yield a negative duration.
     */
    long startTsMs() {
        long start = parentService == null ? originTsMs : parentTsMs;
        return Math.min(start, tsMs);
    }
}
