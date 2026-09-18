/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.util.List;

import ai.signalroom.kafka.isotope.Isotope;

/**
 * A produce acknowledgement as it comes off the producer's network I/O thread:
 * the raw {@code x-isotope} bytes plus the delivery outcome, not yet decoded.
 *
 * <p>Decoding the JSON is the expensive part, and the I/O thread is the one
 * place it must not happen — slow work there delays every other send on that
 * producer. So the acknowledgement path only captures these values and queues
 * them; {@link #resolve()} runs later, on the writer thread.
 *
 * <p>The failure is kept as two strings rather than the {@link Exception}
 * itself, so a broker outage that fills the queue with failed sends doesn't
 * also pin thousands of stack traces in memory.
 *
 * @param isotopeJson  the header value exactly as sent
 * @param partition    partition written to, or {@code -1}
 * @param offset       offset assigned, or {@code -1}
 * @param errorType    failure's class name, or {@code null} on success
 * @param errorMessage failure's message, or {@code null}
 */
record AckedHop(
        byte[] isotopeJson,
        int partition,
        long offset,
        String errorType,
        String errorMessage) implements QueuedSpan {

    static AckedHop of(byte[] isotopeJson, int partition, long offset, Exception error) {
        return new AckedHop(isotopeJson, partition, offset,
            error == null ? null : error.getClass().getName(),
            error == null ? null : error.getMessage());
    }

    /**
     * Decodes the header into the span event for its last hop — the produce
     * being acknowledged — parented to the hop before it. Returns {@code null}
     * when the trace carries no hops, which a header written by
     * {@code onSend} never does.
     *
     * @throws RuntimeException if the header bytes are not a valid isotope
     */
    SpanEvent resolve() {
        Isotope isotope = Isotope.fromJsonBytes(isotopeJson);
        List<Isotope.Hop> hops = isotope.hops();
        int hopCount = hops.size();
        if (hopCount == 0) {
            return null;
        }
        Isotope.Hop hop = hops.get(hopCount - 1);
        Isotope.Hop parent = hopCount >= 2 ? hops.get(hopCount - 2) : null;
        return SpanEvent.of(isotope, hop.service(), hop.topic(), hop.tsMs(),
                parent, hopCount, SpanEvent.Kind.PRODUCE)
            .acknowledged(partition, offset, errorType, errorMessage);
    }
}
