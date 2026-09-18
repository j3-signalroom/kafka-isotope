/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

/**
 * Static facade that routes isotope span emissions to a registered
 * {@link IsotopeSpanSink} — the span-shaped sibling of {@link IsotopeMetrics},
 * and the seam that keeps {@code kafka-isotope-core} free of any tracing
 * dependency. {@link IsotopeProducerInterceptor} and {@link IsotopeContext} call
 * these static methods, which delegate to whatever sink is installed.
 *
 * <h2>Default and registration</h2>
 * The default sink is {@link NoOpSpanSink} — {@link #isEnabled()} is
 * {@code false} and every {@code record*Span} call is inert, so with spans off a
 * send or acknowledgement costs one enabled check and no span work. The optional {@code kafka-isotope-otel}
 * module's {@code KafkaOtlpSpanSink} calls {@link #register(IsotopeSpanSink)}
 * when it starts, after which each hop is written to the spans topic as OTLP.
 *
 * <h2>Why the emission helpers swallow</h2>
 * {@link #recordHopSpan} runs inside the caller's {@code Producer.send()}, and
 * {@link #recordAcknowledgedHopSpan} on the producer's I/O thread. A
 * sink is contractually required not to throw, but a broken third-party sink
 * must not be able to fail somebody's produce, so the delegation here catches
 * and drops anything that escapes. Metrics need no such guard: they take the
 * same path but predate this seam and are not a plug-in point in practice.
 */
public final class IsotopeSpans {

    // volatile: the producer interceptor reads this from Kafka's send threads.
    private static volatile IsotopeSpanSink sink = NoOpSpanSink.INSTANCE;

    private IsotopeSpans() {}

    /**
     * Installs the active sink. Passing {@code null} restores the no-op sink.
     * Called by {@code kafka-isotope-otel} when its span writer starts.
     */
    public static void register(IsotopeSpanSink newSink) {
        sink = (newSink == null) ? NoOpSpanSink.INSTANCE : newSink;
    }

    /** Restores the no-op sink. Primarily for tests and writer shutdown. */
    public static void reset() {
        sink = NoOpSpanSink.INSTANCE;
    }

    /** The currently installed sink (never {@code null}). */
    public static IsotopeSpanSink sink() {
        return sink;
    }

    /** True once a recording sink is installed; gates emission on the hot path. */
    public static boolean isEnabled() {
        return sink.isEnabled();
    }

    /** @see IsotopeSpanSink#recordHopSpan */
    public static void recordHopSpan(Isotope isotope, String thisService,
            String thisTopic, long hopTsMs) {
        try {
            sink.recordHopSpan(isotope, thisService, thisTopic, hopTsMs);
        } catch (RuntimeException e) {
            // A span is never worth failing the send() it rode in on.
        }
    }

    /** @see IsotopeSpanSink#recordAcknowledgedHopSpan */
    public static void recordAcknowledgedHopSpan(byte[] isotopeJson, int partition,
            long offset, Exception error) {
        try {
            sink.recordAcknowledgedHopSpan(isotopeJson, partition, offset, error);
        } catch (RuntimeException e) {
            // Kafka logs and ignores interceptor exceptions here, but at WARN on
            // every record; a broken sink shouldn't flood the producer's log.
        }
    }

    /** @see IsotopeSpanSink#recordConsumeSpan */
    public static void recordConsumeSpan(Isotope isotope, String consumerService,
            String consumedTopic, long consumeTsMs) {
        try {
            sink.recordConsumeSpan(isotope, consumerService, consumedTopic, consumeTsMs);
        } catch (RuntimeException e) {
            // --- best-effort, same as the marker send it accompanies
        }
    }

    /**
     * Takes a reference on the installed sink, returning {@code true} when a
     * recording sink actually took it. Callers keep that answer and pass it to
     * {@link #release(boolean)}, so a sink registered later never sees an
     * unbalanced release that could close its shared resources early.
     */
    public static boolean acquire() {
        IsotopeSpanSink s = sink;
        if (!s.isEnabled()) {
            return false;
        }
        s.acquire();
        return true;
    }

    /**
     * Releases a reference previously taken by {@link #acquire()}.
     *
     * @param acquired what {@code acquire()} returned; {@code false} is a no-op
     */
    public static void release(boolean acquired) {
        if (acquired) {
            sink.release();
        }
    }
}
