/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

/**
 * The default {@link IsotopeMetricsSink}: every method is inert and
 * {@link #isEnabled()} is {@code false}. This is what {@link IsotopeMetrics}
 * routes to until a real sink is registered, so {@code kafka-isotope-core} carries no
 * metrics dependency and the hot path stays free when metrics are off.
 */
final class NoOpMetricsSink implements IsotopeMetricsSink {

    static final NoOpMetricsSink INSTANCE = new NoOpMetricsSink();

    private NoOpMetricsSink() {}

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public void recordHop(String pipeline, String originService,
            String thisService, String thisTopic, long latencyMs, int hopCount) {
        // no-op
    }

    @Override
    public void recordConsume(String pipeline, String originService,
            String consumerService, String thisTopic, long latencyMs) {
        // no-op
    }

    @Override
    public void recordConsumeAge(String pipeline, String originService,
            String consumerService, String thisTopic, long ageMs) {
        // no-op
    }
}
