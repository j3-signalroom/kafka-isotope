/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

/**
 * Static facade that routes isotope metric emissions to a registered
 * {@link IsotopeMetricsSink}. This is the seam that keeps {@code isotope-core}
 * free of any metrics dependency: {@link IsotopeProducerInterceptor} and
 * {@link IsotopeContext} call these static methods unconditionally, and they
 * delegate to whatever sink is installed.
 *
 * <h2>Default and registration</h2>
 * The default sink is {@link NoOpMetricsSink} — {@link #isEnabled()} is
 * {@code false} and every {@code record*} call is inert, so propagation works
 * with zero metrics overhead. The optional {@code isotope-metrics} module's
 * {@code PrometheusIsotopeMetrics} calls {@link #register(IsotopeMetricsSink)}
 * when its exporter binds, after which emissions flow to Micrometer/Prometheus.
 *
 * <p>The historic contract is preserved: emission is a no-op until a sink that
 * reports {@link IsotopeMetricsSink#isEnabled() isEnabled()} is registered, so
 * the interceptor may call {@link #recordHop} unguarded on every send.
 */
public final class IsotopeMetrics {

    // volatile: the producer interceptor reads this from Kafka's send threads.
    private static volatile IsotopeMetricsSink sink = NoOpMetricsSink.INSTANCE;

    private IsotopeMetrics() {}

    /**
     * Installs the active sink. Passing {@code null} restores the no-op sink.
     * Called by {@code isotope-metrics} when its exporter starts.
     */
    public static void register(IsotopeMetricsSink newSink) {
        sink = (newSink == null) ? NoOpMetricsSink.INSTANCE : newSink;
    }

    /** Restores the no-op sink. Primarily for tests and exporter shutdown. */
    public static void reset() {
        sink = NoOpMetricsSink.INSTANCE;
    }

    /** The currently installed sink (never {@code null}). */
    public static IsotopeMetricsSink sink() {
        return sink;
    }

    /** True once a recording sink is installed; gates emission in {@link #recordHop}. */
    public static boolean isEnabled() {
        return sink.isEnabled();
    }

    /** @see IsotopeMetricsSink#recordHop */
    public static void recordHop(String pipeline, String originService,
            String thisService, String thisTopic, long latencyMs, int hopCount) {
        sink.recordHop(pipeline, originService, thisService, thisTopic, latencyMs, hopCount);
    }

    /** @see IsotopeMetricsSink#recordConsume */
    public static void recordConsume(String pipeline, String originService,
            String consumerService, String thisTopic, long latencyMs) {
        sink.recordConsume(pipeline, originService, consumerService, thisTopic, latencyMs);
    }

    /** @see IsotopeMetricsSink#recordConsumeAge */
    public static void recordConsumeAge(String pipeline, String originService,
            String consumerService, String thisTopic, long ageMs) {
        sink.recordConsumeAge(pipeline, originService, consumerService, thisTopic, ageMs);
    }
}
