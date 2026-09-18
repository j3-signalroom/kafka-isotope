/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Derives OTel span ids for isotope hops.
 *
 * <p>Isotope hops carry no span id — the header format predates spans and
 * changing it would break every deployed reader. So an id is <em>derived</em>
 * instead: the SHA-256 of the trace id plus the hop's own three fields
 * (service, topic, timestamp), truncated to the 8 bytes OTel wants.
 *
 * <p>The point of deriving rather than generating is that any service can
 * compute any hop's id from what the header already carries. A child computes
 * its parent's id from the previous entry in {@code h[]} and the two agree
 * across JVMs, so spans link into a tree with no wire-format change and no
 * shared state. This is also why the digest must stay exactly as it is: change
 * the input encoding and spans written by an older service stop matching the
 * parent ids computed by a newer one.
 *
 * <p>SHA-256 is used for its distribution over a 64-bit truncation, not for any
 * security property; these ids are public observability identifiers.
 */
final class SpanIds {

    static final int SPAN_ID_BYTES = 8;

    private static final byte SEP = 0x00;

    // MessageDigest is not thread-safe; one per thread, reset between uses.
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every Java platform implementation.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    });

    private SpanIds() {}

    /**
     * The span id for the hop {@code (service, topic, tsMs)} of {@code traceId}.
     * Deterministic: same inputs, same id, in any process.
     */
    @SuppressWarnings("java:S4790") // not a security digest — see class javadoc
    static byte[] spanId(byte[] traceId, String service, String topic, long tsMs) {
        MessageDigest md = SHA256.get();
        md.reset();
        if (traceId != null) {
            md.update(traceId);
        }
        md.update(SEP);
        md.update(service.getBytes(StandardCharsets.UTF_8));
        md.update(SEP);
        md.update(topic.getBytes(StandardCharsets.UTF_8));
        md.update(SEP);
        for (int shift = 56; shift >= 0; shift -= 8) {
            md.update((byte) (tsMs >>> shift));
        }

        byte[] full = md.digest();
        byte[] id = new byte[SPAN_ID_BYTES];
        System.arraycopy(full, 0, id, 0, SPAN_ID_BYTES);

        // An all-zero span id is invalid per the OTel spec; nudge the
        // vanishingly unlikely collision with zero into a valid id.
        for (byte b : id) {
            if (b != 0) {
                return id;
            }
        }
        id[SPAN_ID_BYTES - 1] = 1;
        return id;
    }

    /** The span id of this event's own edge. */
    static byte[] spanId(SpanEvent e) {
        return spanId(e.traceId(), e.service(), e.topic(), e.tsMs());
    }

    /**
     * The span id of this event's parent edge, or {@code null} when the event
     * has no predecessor (the origin produce, which becomes a root span).
     */
    static byte[] parentSpanId(SpanEvent e) {
        if (e.parentService() == null) {
            return null;
        }
        return spanId(e.traceId(), e.parentService(), e.parentTopic(), e.parentTsMs());
    }
}
