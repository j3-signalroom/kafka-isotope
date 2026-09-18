/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.util.List;

/**
 * The span seam between {@code kafka-isotope-core} (trace propagation) and any
 * tracing backend — the span-shaped sibling of {@link IsotopeMetricsSink}. Core
 * emits through this interface via the {@link IsotopeSpans} facade, so adopters
 * who only want propagation never pull a tracing dependency.
 *
 * <p>The default sink is {@link NoOpSpanSink} (all methods inert,
 * {@link #isEnabled()} {@code false}). The optional {@code kafka-isotope-otel}
 * module provides a Kafka/OTLP implementation ({@code KafkaOtlpSpanSink}) that
 * writes one OTLP span record per hop to a spans topic, and registers itself via
 * {@link IsotopeSpans#register(IsotopeSpanSink)} when it starts.
 *
 * <h2>Where produce spans come from</h2>
 * On kafka-clients 4.1+ the interceptor emits produce spans once the broker
 * has answered, through {@link #recordAcknowledgedHopSpan}, so a span can carry
 * the partition and offset and mark a failed send as an error. Older clients
 * have no acknowledgement callback that sees the record's headers, so there it
 * falls back to {@link #recordHopSpan} at {@code send()} time. Consume spans
 * always come from {@link #recordConsumeSpan}.
 *
 * <h2>Hot-path contract</h2>
 * Every {@code record*} method runs on a thread the application cares about:
 * {@code recordHopSpan} inside {@code Producer.send()}, and
 * {@code recordAcknowledgedHopSpan} on the producer's network I/O thread, where
 * slow code delays every other send on that producer. Implementations
 * <b>must</b> return promptly and <b>must not</b> throw: hand the work to a
 * bounded queue drained by a background thread, drop on overflow, and swallow
 * every error. A span that cannot be written is worth strictly less than the
 * {@code send()} it would otherwise delay or break.
 *
 * <h2>Lifecycle</h2>
 * {@link #acquire()} and {@link #release()} let a sink reference-count its users
 * so a process-wide resource (the OTLP module's shared span producer) outlives
 * any single interceptor. {@link IsotopeProducerInterceptor#configure} acquires
 * and {@link IsotopeProducerInterceptor#close} releases. Both default to no-ops,
 * so a stateless sink can ignore them entirely.
 *
 * <p>Implementations are called from Kafka send/consume threads and must be
 * thread-safe.
 */
public interface IsotopeSpanSink {

    /**
     * True once this sink is actively recording. Core guards emission on this so
     * that, with the no-op sink (or before the exporter starts), the hot path
     * skips even assembling arguments.
     */
    boolean isEnabled();

    /**
     * Records one produce-edge span for the hop {@code IsotopeProducerInterceptor}
     * has just appended, at {@code send()} time. Used on kafka-clients older than
     * 4.1; newer clients report through {@link #recordAcknowledgedHopSpan}.
     * Sinks should implement both.
     *
     * <p>Called <em>after</em> the append, so {@code isotope.hops()} already ends
     * with this hop and the entry before it (when present) is the parent edge —
     * everything a sink needs to derive a parent span id without any change to
     * the wire format.
     *
     * @param isotope     the in-flight trace, hops ending with the one being recorded
     * @param thisService the service producing this hop
     * @param thisTopic   the topic this hop is produced to
     * @param hopTsMs     the hop's wall-clock timestamp, the same value stamped
     *                    into the hop itself
     */
    void recordHopSpan(Isotope isotope, String thisService, String thisTopic, long hopTsMs);

    /**
     * Records the produce-edge span for a hop once the broker has answered —
     * accepted it, rejected it, or the send failed before it got that far.
     * Called from {@code IsotopeProducerInterceptor.onAcknowledgement} on
     * kafka-clients 4.1+, in place of {@link #recordHopSpan}.
     *
     * <p>This runs on the producer's network I/O thread, which is why it is
     * handed the raw header bytes rather than a decoded {@link Isotope}: a sink
     * should queue them and decode off-thread.
     *
     * <p>The default decodes right here and delegates to {@link #recordHopSpan},
     * discarding the delivery outcome, so a sink that implements only the
     * {@code send()}-time method still sees every hop. Sinks that care about the
     * I/O-thread cost or the outcome override it.
     *
     * @param isotopeJson the {@link Isotope#HEADER_KEY} header value exactly as
     *                    sent; its last hop is this produce, the one before it
     *                    the parent edge
     * @param partition   the partition written to, or {@code -1} when the send
     *                    failed before one was assigned
     * @param offset      the offset the broker assigned, or {@code -1} when the
     *                    record was not written
     * @param error       why the send failed, or {@code null} if the broker
     *                    accepted the record
     */
    default void recordAcknowledgedHopSpan(byte[] isotopeJson, int partition,
            long offset, Exception error) {
        Isotope isotope = Isotope.fromJsonBytes(isotopeJson);
        List<Isotope.Hop> hops = isotope.hops();
        if (hops.isEmpty()) {
            return;
        }
        Isotope.Hop hop = hops.get(hops.size() - 1);
        recordHopSpan(isotope, hop.service(), hop.topic(), hop.tsMs());
    }

    /**
     * Records one consume-edge span, the span-shaped companion to the bipartite
     * consume marker written by {@link IsotopeContext#recordConsume}.
     *
     * <p>Here the last entry of {@code isotope.hops()} is the produce that put
     * the record on {@code consumedTopic} — that is, the parent edge.
     *
     * @param isotope         the trace carried by the consumed record
     * @param consumerService the service consuming the record
     * @param consumedTopic   the topic the record was consumed from
     * @param consumeTsMs     the consume wall-clock timestamp
     */
    void recordConsumeSpan(Isotope isotope, String consumerService,
            String consumedTopic, long consumeTsMs);

    /**
     * Registers one more user of this sink. Called once per
     * {@link IsotopeProducerInterceptor} instance from {@code configure}.
     * Defaults to a no-op for sinks that hold no shared resource.
     */
    default void acquire() {
        // --- no-op
    }

    /**
     * Releases a reference taken by {@link #acquire()}; the last release frees
     * whatever shared resource the sink holds. Called from
     * {@link IsotopeProducerInterceptor#close}. Defaults to a no-op.
     */
    default void release() {
        // --- no-op
    }
}
