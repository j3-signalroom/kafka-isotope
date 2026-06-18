/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.signalroom.kafka.isotope.Isotope;
import ai.signalroom.kafka.isotope.IsotopeContext;
import ai.signalroom.kafka.isotope.IsotopeMetrics;
import ai.signalroom.kafka.isotope.IsotopeMetricsSink;

/**
 * Micrometer/Prometheus implementation of {@link IsotopeMetricsSink} — the
 * optional metrics backend for the three <em>stateless-aggregation</em> isotope
 * reports, a metrics-native alternative to the Flink {@code latency_1m},
 * {@code topology_1m}, and {@code hop_distribution_1m} SQL jobs.
 *
 * <h2>How it wires in</h2>
 * This is a process-wide singleton ({@link #INSTANCE}). Calling {@link #start(int)}
 * (or {@link #ensureRegistry()}) binds a dedicated Prometheus registry and
 * registers the singleton into the core {@link IsotopeMetrics} facade, so the
 * producer interceptor and consume markers in {@code isotope-core} begin routing
 * their emissions here. Until then the core facade uses its no-op sink, so
 * propagation runs with zero metrics overhead.
 *
 * <h2>Why three of seven reports</h2>
 * Those three reports are pure scalar aggregation keyed on bounded-cardinality
 * dimensions (service / topic / hop_count — never {@code trace_id}), so they
 * don't need a stream processor: the producer interceptor already has every
 * value in scope on each {@code send()}, and Prometheus can do the 1-minute
 * windowing at query time ({@code rate()}/{@code increase()}). The remaining
 * four reports (percentiles-merged, coverage, bipartite topology, stuck-trace)
 * are per-{@code trace_id} stateful or absence-of-event problems that Prometheus
 * can't express, so they stay in Flink.
 *
 * <h2>What it emits</h2>
 * One {@link Timer} and one {@link Counter} on the produce side:
 * <ul>
 *   <li>{@value #LATENCY_TIMER} tagged {@code (pipeline, origin_service,
 *       this_service, this_topic)} — origin&rarr;hop latency. Its
 *       {@code count}/{@code sum}/{@code max} cover both the latency report
 *       (avg, max) <em>and</em> the topology produce-edge record count.</li>
 *   <li>{@value #HOP_RECORDS} tagged {@code (pipeline, this_topic, hop_count)} —
 *       one increment per produced record, giving the hop distribution.
 *       {@code hop_count} is bounded by {@link Isotope#MAX_HOPS}.</li>
 * </ul>
 *
 * <p>On the consume side three more, emitted from {@link IsotopeContext}'s
 * marker and adoption paths: {@value #CONSUME_LATENCY_TIMER} (time-to-consume),
 * {@value #CONSUME_RECORDS} (topic&rarr;consumer edge counts — the consume half
 * of the bipartite topology), and {@value #CONSUME_AGE_TIMER} (origin&rarr;consume
 * age, emitted once on every consuming stage).
 *
 * <h2>Two deliberate gaps vs. the Flink reports</h2>
 * <ul>
 *   <li><b>No {@code distinct_traces}.</b> A counter can't dedup and
 *       {@code trace_id} is unbounded-cardinality.</li>
 *   <li><b>No windowed {@code min} latency.</b> A {@code Timer} exposes max but
 *       not a per-window min.</li>
 * </ul>
 */
public final class PrometheusIsotopeMetrics implements IsotopeMetricsSink {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusIsotopeMetrics.class);

    /**
     * Per-hop latency timer (origin&rarr;this-hop). Its {@code count} also
     * serves as the topology produce-edge record count.
     */
    static final String LATENCY_TIMER = "isotope.hop.latency";

    /** Records per {@code (pipeline, this_topic, hop_count)} — hop distribution. */
    static final String HOP_RECORDS = "isotope.hop.records";

    /**
     * Origin&rarr;consume latency timer, tagged {@code (pipeline, origin_service,
     * consumer_service, this_topic)} — a stateless, metrics-native
     * time-to-consume figure with no Flink-report counterpart.
     */
    static final String CONSUME_LATENCY_TIMER = "isotope.consume.latency";

    /**
     * Records per {@code (pipeline, this_topic, consumer_service)} — the consume
     * half of the bipartite topology (topic&rarr;consumer edge counts).
     */
    static final String CONSUME_RECORDS = "isotope.consume.records";

    /**
     * Origin&rarr;consume "age" timer, tagged {@code (pipeline, origin_service,
     * consumer_service, this_topic)} — how stale a record was when consumed.
     * Emitted once on every consuming stage, making it the universal
     * consume-side age signal.
     */
    static final String CONSUME_AGE_TIMER = "isotope.consume.age";

    /** Process-wide singleton; registered into {@link IsotopeMetrics} on start. */
    public static final PrometheusIsotopeMetrics INSTANCE = new PrometheusIsotopeMetrics();

    // The exporter owns a dedicated Prometheus registry rather than the
    // process-global one: the meters here are the only thing on the /metrics
    // endpoint, and a self-contained registry keeps binding/teardown (and the
    // unit test) trivial. Both fields are volatile — recorded from Kafka threads.
    private volatile PrometheusMeterRegistry registry;
    private volatile HttpServer server;

    private PrometheusIsotopeMetrics() {}

    // ------------------------------------------------------------------
    // Lifecycle (static, adopter/demo-facing)
    // ------------------------------------------------------------------

    /**
     * Idempotently binds the Prometheus registry, serves it at {@code GET
     * /metrics} on {@code port}, and registers this sink into the core
     * {@link IsotopeMetrics} facade. Safe to call more than once — only the
     * first call opens the listener.
     *
     * <p>The exporter is an optional sidecar, so a failure to bind {@code port}
     * (commonly a sibling stage already holding the default 9404) is logged at
     * WARN and swallowed — nothing is registered, {@link #isEnabled()} stays
     * {@code false}, and the caller's data pipeline runs on unaffected. It does
     * <em>not</em> throw.
     */
    public static synchronized void start(int port) {
        INSTANCE.startInternal(port);
    }

    /** Binds the Prometheus registry (and registers the sink) without serving HTTP. Idempotent. */
    static synchronized void ensureRegistry() {
        INSTANCE.ensureRegistryInternal();
    }

    /** Current Prometheus exposition text, or {@code ""} before the registry binds. */
    static String scrape() {
        return INSTANCE.scrapeInternal();
    }

    /** Test-only: stop the server, close the registry, reset state, and restore the no-op sink. */
    static synchronized void resetForTest() {
        INSTANCE.resetInternal();
    }

    // ------------------------------------------------------------------
    // IsotopeMetricsSink (instance)
    // ------------------------------------------------------------------

    /** True once the registry is bound; gates emission. */
    @Override
    public boolean isEnabled() {
        return registry != null;
    }

    /**
     * Emits the stateless-aggregation metrics for one produced hop. No-op
     * unless the exporter has been bound (via {@link #start(int)}).
     */
    @Override
    public void recordHop(String pipeline, String originService,
            String thisService, String thisTopic, long latencyMs, int hopCount) {
        PrometheusMeterRegistry r = registry;
        if (r == null) return;

        Timer.builder(LATENCY_TIMER)
            .tag("pipeline",       pipeline)
            .tag("origin_service", originService)
            .tag("this_service",   thisService)
            .tag("this_topic",     thisTopic)
            .register(r)
            .record(Math.max(0L, latencyMs), TimeUnit.MILLISECONDS);

        Counter.builder(HOP_RECORDS)
            .tag("pipeline",   pipeline)
            .tag("this_topic", thisTopic)
            .tag("hop_count",  Integer.toString(hopCount))
            .register(r)
            .increment();
    }

    /**
     * Emits the stateless consume-edge metrics for one consume marker. No-op
     * unless the exporter has been bound. The companion to {@link #recordHop}
     * on the consume side.
     */
    @Override
    public void recordConsume(String pipeline, String originService,
            String consumerService, String thisTopic, long latencyMs) {
        PrometheusMeterRegistry r = registry;
        if (r == null) return;

        if (latencyMs >= 0) {
            Timer.builder(CONSUME_LATENCY_TIMER)
                .tag("pipeline",         pipeline)
                .tag("origin_service",   originService)
                .tag("consumer_service", consumerService)
                .tag("this_topic",       thisTopic)
                .register(r)
                .record(latencyMs, TimeUnit.MILLISECONDS);
        }

        Counter.builder(CONSUME_RECORDS)
            .tag("pipeline",         pipeline)
            .tag("this_topic",       thisTopic)
            .tag("consumer_service", consumerService)
            .register(r)
            .increment();
    }

    /**
     * Emits the stateless consume-side "age" timer ({@code now - originTs}) for
     * one consumed record. No-op unless the exporter has been bound.
     */
    @Override
    public void recordConsumeAge(String pipeline, String originService,
            String consumerService, String thisTopic, long ageMs) {
        PrometheusMeterRegistry r = registry;
        if (r == null) return;

        Timer.builder(CONSUME_AGE_TIMER)
            .tag("pipeline",         pipeline)
            .tag("origin_service",   originService)
            .tag("consumer_service", consumerService)
            .tag("this_topic",       thisTopic)
            .register(r)
            .record(Math.max(0L, ageMs), TimeUnit.MILLISECONDS);
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    private void startInternal(int port) {
        if (server != null) return;

        // Open the listener BEFORE binding the registry, so a bind failure
        // leaves nothing half-wired (no registry accumulating meters that
        // nothing can scrape, no sink registered into the core facade).
        HttpServer s;
        try {
            s = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            LOG.warn("isotope metrics exporter NOT started: port {} unavailable ({}). "
                + "Another stage may already own it — set -Dmetrics.prometheus.port "
                + "to a free port. Continuing without metrics.", port, e.toString());
            return;
        }

        ensureRegistryInternal();
        s.createContext("/metrics", exchange -> {
            byte[] body = scrapeInternal().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders()
                .add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        s.start();
        server = s;
        LOG.info("isotope metrics exporter listening on http://0.0.0.0:{}/metrics", port);
    }

    private void ensureRegistryInternal() {
        if (registry == null) {
            registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        }
        // Route core's emissions to this sink now that we can record.
        IsotopeMetrics.register(this);
    }

    private String scrapeInternal() {
        PrometheusMeterRegistry r = registry;
        return r == null ? "" : r.scrape();
    }

    private void resetInternal() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
        if (registry != null) {
            registry.close();
            registry = null;
        }
        IsotopeMetrics.reset();
    }
}
