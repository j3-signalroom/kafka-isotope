/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ai.signalroom.kafka.isotope.Isotope;
import ai.signalroom.kafka.isotope.IsotopeSpanSink;
import ai.signalroom.kafka.isotope.IsotopeSpans;

/**
 * Kafka/OTLP implementation of {@link IsotopeSpanSink} — the optional tracing
 * backend that turns each isotope hop into an OpenTelemetry span and writes it
 * to a Kafka topic as an OTLP {@code ExportTraceServiceRequest}.
 *
 * <h2>Why a topic, and why OTLP bytes</h2>
 * The spans land on {@value #DEFAULT_TOPIC} in exactly the format a stock
 * OpenTelemetry Collector reads: a {@code kafka} receiver with
 * {@code encoding: otlp_proto} consumes the topic and exports to any OTLP
 * backend. Nothing isotope-specific runs on the consuming side — the whole
 * integration is Collector config plus read access to one topic.
 *
 * <h2>How it wires in</h2>
 * A process-wide singleton ({@link #INSTANCE}). {@link #start(Map)} builds the
 * span producer and registers the singleton into the core {@link IsotopeSpans}
 * facade, after which {@code IsotopeProducerInterceptor} and
 * {@code IsotopeContext.recordConsume} route their edges here. Until then core
 * uses its no-op sink and propagation runs with zero span overhead.
 *
 * <h2>Its own producer</h2>
 * The interceptor cannot write through the producer it is attached to — that
 * producer is mid-{@code send()} and its interceptor chain would stamp and trace
 * the span records themselves. So this sink owns a separate producer, shared by
 * every interceptor in the JVM, built with {@code interceptor.classes} stripped
 * so its writes are never traced. The consume-marker producer is separate for
 * the same reason, the difference being that one is the adopter's to manage and
 * this one is ours.
 *
 * <h2>Never in the way of a send</h2>
 * {@link #recordHopSpan} runs on the caller's thread inside {@code send()}, so
 * it does exactly one thing: drop a small {@link SpanEvent} on a bounded queue
 * ({@value #DEFAULT_QUEUE_CAPACITY} by default) and return. A background daemon
 * thread does the hashing, the protobuf encoding and the Kafka write. When the
 * queue is full, spans are dropped and counted ({@link #droppedSpans()}) rather
 * than made to wait: telemetry yields to the pipeline, never the other way
 * around. Every error on the path is swallowed and logged, throttled so a broker
 * outage can't turn into a log flood.
 *
 * <h2>Produce spans come from onSend</h2>
 * Spans are emitted from {@code onSend}, not {@code onAcknowledgement}, because
 * the acknowledgement callback receives neither the record nor its headers —
 * matching an ack back to its trace would need a map of in-flight records keyed
 * by correlation id. That means a span records an <em>attempted</em> produce:
 * a record that is later dropped by the producer still has a span. Carrying
 * broker-assigned partition and offset (and dropping spans for failed sends)
 * waits for that in-flight map, which stays out of scope here.
 *
 * <h2>Lifecycle</h2>
 * {@link #start(Map)} takes the first reference, and each configured
 * interceptor takes one more; {@code close()} on an interceptor hands one back,
 * and {@link #close()} hands back the starter's. The producer shuts down when
 * the last reference is released. An interceptor's {@code close()} therefore
 * cannot pull the producer out from under its siblings — a real hazard for a
 * JVM-wide resource that every interceptor shares.
 */
public final class KafkaOtlpSpanSink implements IsotopeSpanSink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOtlpSpanSink.class);

    /** Default topic the span records are written to. */
    public static final String DEFAULT_TOPIC = "isotope_trace_spans";

    /** Default bound on queued-but-unwritten spans. */
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    /** Process-wide singleton; registered into {@link IsotopeSpans} on start. */
    public static final KafkaOtlpSpanSink INSTANCE = new KafkaOtlpSpanSink();

    /** Spans per OTLP request; caps the size of any single record on the topic. */
    private static final int MAX_BATCH = 512;

    /** How long the writer waits for the first span of a batch before looping. */
    private static final long POLL_MS = 200L;

    /** Shutdown grace for the writer thread and the producer. */
    private static final long WORKER_JOIN_MS = 2_000L;
    private static final Duration PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(5);

    /** Minimum gap between two "spans were dropped" warnings. */
    private static final long WARN_INTERVAL_MS = 60_000L;

    // Read from Kafka send threads, written under the class lock.
    private volatile Producer<byte[], byte[]> producer;
    private volatile BlockingQueue<SpanEvent> queue;
    private volatile String topic = DEFAULT_TOPIC;
    private volatile boolean running;

    // Guarded by the class lock (all lifecycle goes through static synchronized).
    private Thread worker;
    private boolean starterRefHeld;

    private final AtomicInteger refs = new AtomicInteger();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong sendFailures = new AtomicLong();
    private final AtomicLong lastDropWarnMs = new AtomicLong();
    private final AtomicLong lastFailWarnMs = new AtomicLong();

    private KafkaOtlpSpanSink() {}

    // ------------------------------------------------------------------
    // Lifecycle (static, adopter-facing)
    // ------------------------------------------------------------------

    /**
     * Starts the span writer against {@value #DEFAULT_TOPIC} with the default
     * queue bound. See {@link #start(Map, String, int)}.
     *
     * @param producerConfig producer config for the sink's own producer; needs
     *                       at least {@code bootstrap.servers} (plus whatever
     *                       security settings the cluster requires)
     */
    public static void start(Map<String, ?> producerConfig) {
        start(producerConfig, DEFAULT_TOPIC, DEFAULT_QUEUE_CAPACITY);
    }

    /** Starts the span writer against {@code topic}. See {@link #start(Map, String, int)}. */
    public static void start(Map<String, ?> producerConfig, String topic) {
        start(producerConfig, topic, DEFAULT_QUEUE_CAPACITY);
    }

    /**
     * Idempotently builds the span producer, starts the background writer, and
     * registers this sink into the core {@link IsotopeSpans} facade. Safe to
     * call more than once — only the first call binds.
     *
     * <p>Like the Prometheus exporter, this is an optional sidecar: if the
     * producer cannot be built (bad or missing {@code bootstrap.servers}, a
     * security misconfiguration), the failure is logged at WARN and swallowed —
     * nothing is registered, {@link #isEnabled()} stays {@code false}, and the
     * adopter's pipeline runs on unaffected. It does <em>not</em> throw.
     *
     * @param producerConfig producer config; {@code interceptor.classes} is
     *                       stripped and the serializers are forced to
     *                       {@code ByteArraySerializer}
     * @param topic          topic to write span records to
     * @param queueCapacity  maximum spans held in memory awaiting write
     */
    public static synchronized void start(
            Map<String, ?> producerConfig, String topic, int queueCapacity) {
        INSTANCE.startInternal(producerConfig, topic, queueCapacity, null);
    }

    /**
     * Hands back the reference taken by {@link #start}. The writer shuts down
     * once every interceptor has closed too; if any are still open, this only
     * relinquishes the starter's claim and the last one out does the teardown.
     */
    public static synchronized void close() {
        INSTANCE.closeInternal();
    }

    /** Spans discarded because the queue was full — the cost of never blocking a send. */
    public static long droppedSpans() {
        return INSTANCE.dropped.get();
    }

    /** Span records the producer failed to deliver. */
    public static long failedSpanSends() {
        return INSTANCE.sendFailures.get();
    }

    /** Test hook: start against a caller-supplied producer (e.g. a MockProducer). */
    static synchronized void startWithProducer(
            Producer<byte[], byte[]> producer, String topic, int queueCapacity) {
        INSTANCE.startInternal(null, topic, queueCapacity, producer);
    }

    /** Test hook: tear everything down and restore the no-op sink. */
    static synchronized void resetForTest() {
        INSTANCE.shutdownInternal();
        INSTANCE.dropped.set(0);
        INSTANCE.sendFailures.set(0);
        INSTANCE.lastDropWarnMs.set(0);
        INSTANCE.lastFailWarnMs.set(0);
        IsotopeSpans.reset();
    }

    // ------------------------------------------------------------------
    // IsotopeSpanSink
    // ------------------------------------------------------------------

    /** True once the writer is running; gates emission in core. */
    @Override
    public boolean isEnabled() {
        return running;
    }

    /**
     * Queues the produce edge for the hop just appended. The hop is the last
     * entry of {@code isotope.hops()} and its parent is the entry before it.
     */
    @Override
    public void recordHopSpan(Isotope isotope, String thisService, String thisTopic, long hopTsMs) {
        BlockingQueue<SpanEvent> q = queue;
        if (q == null || isotope == null) {
            return;
        }
        try {
            List<Isotope.Hop> hops = isotope.hops();
            int hopCount = hops.size();
            Isotope.Hop parent = hopCount >= 2 ? hops.get(hopCount - 2) : null;
            offer(q, SpanEvent.of(isotope, thisService, thisTopic, hopTsMs,
                parent, hopCount, SpanEvent.Kind.PRODUCE));
        } catch (RuntimeException e) {
            // Belt and braces: nothing here may escape into somebody's send().
            dropped.incrementAndGet();
        }
    }

    /**
     * Queues the consume edge. Its parent is the last hop on the trace — the
     * produce that put the record on {@code consumedTopic}.
     */
    @Override
    public void recordConsumeSpan(Isotope isotope, String consumerService,
            String consumedTopic, long consumeTsMs) {
        BlockingQueue<SpanEvent> q = queue;
        if (q == null || isotope == null) {
            return;
        }
        try {
            List<Isotope.Hop> hops = isotope.hops();
            int hopCount = hops.size();
            Isotope.Hop parent = hopCount >= 1 ? hops.get(hopCount - 1) : null;
            offer(q, SpanEvent.of(isotope, consumerService, consumedTopic, consumeTsMs,
                parent, hopCount, SpanEvent.Kind.CONSUME));
        } catch (RuntimeException e) {
            dropped.incrementAndGet();
        }
    }

    /** Registers one more holder of the shared producer. */
    @Override
    public void acquire() {
        refs.incrementAndGet();
    }

    /** Releases a reference; the last one out shuts the writer down. */
    @Override
    public void release() {
        if (!running) {
            return;
        }
        if (refs.decrementAndGet() <= 0) {
            shutdownShared();
        }
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    private static synchronized void shutdownShared() {
        INSTANCE.shutdownInternal();
    }

    private void startInternal(Map<String, ?> producerConfig, String topicName,
            int queueCapacity, Producer<byte[], byte[]> suppliedProducer) {
        if (running) {
            return;
        }

        Producer<byte[], byte[]> p = suppliedProducer;
        if (p == null) {
            try {
                p = new KafkaProducer<>(spanProducerConfig(producerConfig));
            } catch (RuntimeException e) {
                LOG.warn("isotope span writer NOT started: could not build the span producer ({}). "
                    + "Check bootstrap.servers and security settings. Continuing without spans.",
                    e.toString());
                return;
            }
        }

        topic = (topicName == null || topicName.isBlank()) ? DEFAULT_TOPIC : topicName;
        BlockingQueue<SpanEvent> q =
            new ArrayBlockingQueue<>(queueCapacity > 0 ? queueCapacity : DEFAULT_QUEUE_CAPACITY);
        queue = q;
        producer = p;
        running = true;

        // The writer holds its own references to the queue and producer, so
        // teardown nulling the fields can't trip it mid-batch.
        final Producer<byte[], byte[]> writerProducer = p;
        Thread t = new Thread(() -> drainLoop(q, writerProducer), "isotope-span-writer");
        t.setDaemon(true);
        t.start();
        worker = t;

        starterRefHeld = true;
        refs.set(1);
        IsotopeSpans.register(this);
        LOG.info("isotope span writer started: OTLP spans -> topic \"{}\" (queue capacity {})",
            topic, q.remainingCapacity());
    }

    /**
     * The span producer's config: the adopter's settings, with the parts that
     * would make it trace itself or block a caller overridden.
     */
    static Map<String, Object> spanProducerConfig(Map<String, ?> in) {
        Map<String, Object> c = in == null ? new HashMap<>() : new HashMap<>(in);

        // Non-negotiable: an interceptor chain on this producer would stamp the
        // span records themselves, tracing the tracer.
        c.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
        c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Defaults tuned for telemetry: cheap acks, no idempotence bookkeeping,
        // a little batching, and a bounded wait for metadata so the writer
        // thread can't park indefinitely against an unreachable broker. All
        // overridable — anything the caller sets explicitly is left alone.
        c.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "isotope-span-writer");
        c.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        c.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        c.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, 50);
        c.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1_000);
        return c;
    }

    private void offer(BlockingQueue<SpanEvent> q, SpanEvent event) {
        if (!q.offer(event)) {
            long total = dropped.incrementAndGet();
            warnThrottled(lastDropWarnMs,
                "isotope span queue full — {} span(s) dropped so far. The writer is behind; "
                + "check broker reachability or raise the queue capacity.", total);
        }
    }

    private void drainLoop(BlockingQueue<SpanEvent> q, Producer<byte[], byte[]> p) {
        List<SpanEvent> batch = new ArrayList<>(MAX_BATCH);
        while (running) {
            try {
                SpanEvent first = q.poll(POLL_MS, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);
                q.drainTo(batch, MAX_BATCH - 1);
                flush(batch, p);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                warnThrottled(lastFailWarnMs, "isotope span writer error ({}); spans dropped.", e);
            } finally {
                batch.clear();
            }
        }
    }

    /**
     * Encodes and sends one batch, grouped by trace so each record holds a
     * single trace's spans and can be keyed by trace id — which also keeps a
     * trace's spans on one partition, and so in order, for the Collector.
     */
    private void flush(List<SpanEvent> batch, Producer<byte[], byte[]> p) {
        Map<ByteBuffer, List<SpanEvent>> byTrace = new LinkedHashMap<>();
        for (SpanEvent e : batch) {
            byTrace.computeIfAbsent(ByteBuffer.wrap(e.traceId()), k -> new ArrayList<>()).add(e);
        }

        String destination = topic;
        for (Map.Entry<ByteBuffer, List<SpanEvent>> entry : byTrace.entrySet()) {
            byte[] traceId = entry.getKey().array();
            try {
                byte[] payload = OtlpSpanEncoder.encode(entry.getValue());
                p.send(new ProducerRecord<>(destination, traceId, payload), (metadata, exception) -> {
                    if (exception != null) {
                        long total = sendFailures.incrementAndGet();
                        warnThrottled(lastFailWarnMs,
                            "isotope span send failed ({} failure(s) so far): {}",
                            total, exception.toString());
                    }
                });
            } catch (RuntimeException e) {
                long total = sendFailures.incrementAndGet();
                warnThrottled(lastFailWarnMs,
                    "isotope span send rejected ({} failure(s) so far): {}", total, e.toString());
            }
        }
    }

    private void closeInternal() {
        if (!running) {
            return;
        }
        if (starterRefHeld) {
            starterRefHeld = false;
            if (refs.decrementAndGet() > 0) {
                // Interceptors are still using the writer; the last one shuts it down.
                return;
            }
        }
        shutdownInternal();
    }

    private void shutdownInternal() {
        if (!running) {
            return;
        }
        running = false;
        IsotopeSpans.reset();

        Thread t = worker;
        worker = null;
        if (t != null) {
            t.interrupt();
            try {
                t.join(WORKER_JOIN_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        Producer<byte[], byte[]> p = producer;
        BlockingQueue<SpanEvent> q = queue;
        producer = null;
        queue = null;
        starterRefHeld = false;
        refs.set(0);

        if (p == null) {
            return;
        }
        try {
            // One last drain, here rather than in the (interrupted) writer, so
            // shutdown deterministically flushes what was already queued.
            if (q != null && !q.isEmpty()) {
                List<SpanEvent> remaining = new ArrayList<>(q.size());
                q.drainTo(remaining);
                for (int i = 0; i < remaining.size(); i += MAX_BATCH) {
                    flush(remaining.subList(i, Math.min(i + MAX_BATCH, remaining.size())), p);
                }
            }
            p.flush();
        } catch (RuntimeException e) {
            LOG.debug("isotope span writer: final flush failed ({})", e.toString());
        } finally {
            try {
                p.close(PRODUCER_CLOSE_TIMEOUT);
            } catch (RuntimeException e) {
                LOG.debug("isotope span writer: producer close failed ({})", e.toString());
            }
            LOG.info("isotope span writer stopped ({} span(s) dropped, {} send failure(s))",
                dropped.get(), sendFailures.get());
        }
    }

    /** Logs at WARN at most once per {@value #WARN_INTERVAL_MS} ms. */
    private void warnThrottled(AtomicLong lastWarnMs, String message, Object... args) {
        long now = System.currentTimeMillis();
        long last = lastWarnMs.get();
        if (now - last >= WARN_INTERVAL_MS && lastWarnMs.compareAndSet(last, now)) {
            LOG.warn(message, args);
        }
    }
}
