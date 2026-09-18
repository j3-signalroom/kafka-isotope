/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

/**
 * The default {@link IsotopeSpanSink}: every method is inert and
 * {@link #isEnabled()} is {@code false}. This is what {@link IsotopeSpans}
 * routes to until a real sink is registered, so {@code kafka-isotope-core}
 * carries no tracing dependency and the hot path stays free when spans are off.
 */
final class NoOpSpanSink implements IsotopeSpanSink {

    static final NoOpSpanSink INSTANCE = new NoOpSpanSink();

    private NoOpSpanSink() {}

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void recordHopSpan(Isotope isotope, String thisService, String thisTopic, long hopTsMs) {
        // --- no-op
    }

    @Override
    public void recordAcknowledgedHopSpan(byte[] isotopeJson, int partition,
            long offset, Exception error) {
        // --- no-op (overridden so the default's JSON decode never runs)
    }

    @Override
    public void recordConsumeSpan(Isotope isotope, String consumerService,
            String consumedTopic, long consumeTsMs) {
        // --- no-op
    }
}
