/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

/**
 * The metrics seam between {@code kafka-isotope-core} (trace propagation) and any
 * metrics backend. Core emits through this interface via the {@link IsotopeMetrics}
 * facade, so adopters who only want propagation never pull a metrics dependency.
 *
 * <p>The default sink is {@link NoOpMetricsSink} (all methods inert,
 * {@link #isEnabled()} {@code false}). The optional {@code kafka-isotope-metrics}
 * module provides a Micrometer/Prometheus implementation
 * ({@code PrometheusIsotopeMetrics}) and registers it via
 * {@link IsotopeMetrics#register(IsotopeMetricsSink)} when its exporter starts.
 *
 * <p>Method contracts mirror the original meters — see {@link IsotopeMetrics}.
 * Implementations are called from Kafka send/consume threads and must be
 * thread-safe.
 */
public interface IsotopeMetricsSink {

    /**
     * True once this sink is actively recording. Core guards emission on this so
     * that, with the no-op sink (or before the exporter starts), the hot path
     * skips building tag values.
     */
    boolean isEnabled();

    /**
     * Records the stateless-aggregation metrics for one produced hop:
     * origin&rarr;hop latency plus the hop-distribution counter.
     *
     * @param pipeline       the trace's pipeline (origin-set, forwarded)
     * @param originService  the service that originated the trace
     * @param thisService    the service producing this hop
     * @param thisTopic      the topic this hop is produced to
     * @param latencyMs      {@code thisHopTs - originTs}; clamp at 0 for skew
     * @param hopCount       number of hops accumulated including this one
     */
    void recordHop(String pipeline, String originService,
            String thisService, String thisTopic, long latencyMs, int hopCount);

    /**
     * Records the consume-edge count and origin&rarr;consume latency for one
     * consume marker.
     *
     * @param latencyMs {@code consumeTs - originTs}, or {@code < 0} when the
     *                  record carried no origin timestamp (edge still counted,
     *                  latency timer skipped)
     */
    void recordConsume(String pipeline, String originService,
            String consumerService, String thisTopic, long latencyMs);

    /**
     * Records the consume-side "age" timer ({@code now - originTs}) for one
     * consumed record — emitted once on every consuming stage.
     *
     * @param ageMs {@code now - originTs}; clamp at 0 for skew
     */
    void recordConsumeAge(String pipeline, String originService,
            String consumerService, String thisTopic, long ageMs);
}
