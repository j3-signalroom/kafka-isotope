/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer-side half of the isotope tracer.
 *
 * <p>On every {@code send()}, this interceptor finds (or creates) the in-flight
 * {@link Isotope}, appends a hop describing this produce edge, and writes the
 * JSON-encoded isotope plus seven scalar headers describing the just-appended
 * hop. Sourcing order: thread-local context, inbound header, or a fresh trace
 * stamped with {@value #SERVICE_NAME_CONFIG}.
 *
 * <p>When the optional span writer is running, each hop also becomes a span.
 * On kafka-clients 4.1+ the span is emitted from
 * {@link #onAcknowledgement(RecordMetadata, Exception, Headers)} once the broker
 * has answered, so it records the delivery outcome; on older clients, which
 * never show an acknowledgement the record's headers, it is emitted from
 * {@link #onSend} instead.
 */
public class IsotopeProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    public static final String SERVICE_NAME_CONFIG = "isotope.service.name";

    /**
     * Names the logical pipeline a fresh trace belongs to (e.g. {@code orders}
     * vs {@code location}). Only the trace's origin uses this value — it is
     * stamped once at {@link Isotope#newTrace} and then forwarded unchanged on
     * every hop, so downstream services inherit it from the inbound record and
     * never need to set it.
     */
    public static final String PIPELINE_NAME_CONFIG = "isotope.pipeline.name";

    private static final Logger LOG = LoggerFactory.getLogger(IsotopeProducerInterceptor.class);

    private String serviceName = "unknown";
    private String pipelineName = "unknown";

    /*
     * Whether this interceptor holds a reference on the span sink. Recorded at
     * configure() time and handed back in close(): the OTLP sink's producer is
     * process-wide and shared by every interceptor instance, so it may only be
     * torn down once the last of them is gone. False whenever spans were off
     * when this interceptor was configured, which keeps a sink registered later
     * from ever seeing a release it never handed out.
     */
    private boolean spansAcquired;

    /**
     * Whether the running kafka-clients passes the sent record's headers to
     * {@code onAcknowledgement} — the three-argument overload added in 4.1. Every
     * 4.1+ producer calls only that overload, so its presence on the interface
     * is proof it will be called. Resolved once: the interface can't change
     * under a loaded class.
     */
    static final boolean ACK_HEADERS_SUPPORTED = ackHeadersSupported();

    /*
     * Where hop spans are emitted: from onAcknowledgement when the client
     * supports it, otherwise from onSend. Exactly one of the two emits, so a hop
     * never produces two spans. Package-private so tests can exercise the onSend
     * fallback on a 4.1+ test classpath.
     */
    boolean spansOnAck = ACK_HEADERS_SUPPORTED;

    /**
     * This method is called once per interceptor instance, which is typically once per producer. It reads
     * {@value #SERVICE_NAME_CONFIG} and {@value #PIPELINE_NAME_CONFIG} from the provided configs map, and
     * logs a warning if either is missing or blank. The configured service name is used to tag hops in the
     * isotope trace, while the pipeline name is used to tag new traces. If not configured, both default to
     * "unknown".
     */
    @Override
    public void configure(Map<String, ?> configs) {
        Object v = configs.get(SERVICE_NAME_CONFIG);
        if (v instanceof String s && !s.isBlank()) {
            serviceName = s;
        } else {
            LOG.warn("{} not configured; isotope hops will be tagged service=\"unknown\"",
                SERVICE_NAME_CONFIG);
        }

        Object pn = configs.get(PIPELINE_NAME_CONFIG);
        if (pn instanceof String s && !s.isBlank()) {
            pipelineName = s;
        } else {
            LOG.warn("{} not configured; new traces will be tagged pipeline=\"unknown\"",
                PIPELINE_NAME_CONFIG);
        }

        // Claim a reference on the span sink (a no-op unless the optional
        // kafka-isotope-otel writer is running), released in close().
        spansAcquired = IsotopeSpans.acquire();
    }

    /**
     * This method is called once per record sent. It finds or creates the in-flight isotope, appends a hop
     * describing this produce edge, and writes the JSON-encoded isotope plus seven scalar headers describing
     * the just-appended hop. The sourcing order for the in-flight isotope is: thread-local context, inbound
     * header, or a fresh trace. The hop timestamp is the current system time in milliseconds. The scalar headers are:
     * - {@code isotope-trace-id}: the trace ID in hex, same as the "t" field in the JSON.
     * - {@code isotope-origin-ts}: the origin timestamp in milliseconds, same as the "o" field in the JSON.
     * - {@code isotope-origin-service}: the origin service name, same as the "s" field in the JSON.
     * - {@code isotope-pipeline}: the pipeline name, same as the "p" field in the JSON (or "unknown" if the trace was
     * adopted from a pre-pipeline record).
     * - {@code isotope-this-service}: the service name of this hop, same as the "n" field in the JSON.
     * - {@code isotope-this-topic}: the topic being produced to, same as the "t" field in the hop object in the JSON.
     * - {@code isotope-hop-count}: the total number of hops in the trace so far, including the just-appended one, same 
     * as the length of the "h" array in the JSON.
     * 
     * The JSON-encoded isotope is written to the header key {@code isotope}, and the scalar headers are written with
     * UTF-8 encoding. If Micrometer metrics are enabled, this method also emits a hop report with the latency from origin
     * to this hop and the total hop count.
     */
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> producerRecord) {
        Isotope iso = IsotopeContext.current();
        if (iso == null) {
            iso = Isotope.fromHeaders(producerRecord.headers());
        }
        if (iso == null) {
            iso = Isotope.newTrace(serviceName, pipelineName);
        }

        long hopTsMs = System.currentTimeMillis();
        iso.appendHop(new Isotope.Hop(serviceName, producerRecord.topic(), hopTsMs));

        Headers h = producerRecord.headers();
        h.remove(Isotope.HEADER_KEY);
        h.add(Isotope.HEADER_KEY, iso.toJsonBytes());

        /*
         * Scalar reporting headers - overwritten on every hop so each record
         * carries the most-recent-hop scalars.
         */
        putString(h, Isotope.HEADER_TRACE_ID,       iso.traceIdHex());
        putString(h, Isotope.HEADER_ORIGIN_TS,      Long.toString(iso.originTsMs()));
        putString(h, Isotope.HEADER_ORIGIN_SERVICE, iso.originService());

        /*
         * pipeline() can be null only for traces adopted from a pre-pipeline
         * record (legacy JSON without "p"); fall back to "unknown" so the
         * scalar header is always present for Flink SQL.
         */
        putString(h, Isotope.HEADER_PIPELINE,       Objects.requireNonNullElse(iso.pipeline(), "unknown"));
        putString(h, Isotope.HEADER_THIS_SERVICE,   serviceName);
        putString(h, Isotope.HEADER_THIS_TOPIC,     producerRecord.topic());
        putString(h, Isotope.HEADER_HOP_COUNT,      Integer.toString(iso.hops().size()));

        /*
         * Emit the stateless-aggregation reports (latency / topology /
         * hop distribution) to Micrometer for Prometheus/Grafana. No-op
         * unless the app started the exporter (IsotopeMetrics.start). Latency
         * is the partial origin→this-hop figure, same as the Flink latency
         * report's `broker_ts_at_this_hop - origin_ts`.
         */
        if (IsotopeMetrics.isEnabled()) {
            IsotopeMetrics.recordHop(
                Objects.requireNonNullElse(iso.pipeline(), "unknown"),
                iso.originService(),
                serviceName,
                producerRecord.topic(),
                hopTsMs - iso.originTsMs(),
                iso.hops().size());
        }

        /*
         * Pre-4.1 clients only: emit the span for the hop just appended. On 4.1+
         * it waits for onAcknowledgement, where the outcome is known. No-op
         * unless the optional kafka-isotope-otel writer is running, and when it
         * is, this only drops a small event on a bounded queue that a background
         * thread drains — onSend runs on the caller's thread and never waits on
         * a span.
         */
        if (!spansOnAck && IsotopeSpans.isEnabled()) {
            IsotopeSpans.recordHopSpan(iso, serviceName, producerRecord.topic(), hopTsMs);
        }

        return producerRecord;
    }

    /**
     * This method writes a UTF-8 string header, first removing any existing header with the same key to avoid
     * duplicates. Kafka's Headers API allows multiple headers with the same key, but for our scalar reporting
     * headers we want to ensure there is only one header with a given key.
     *
     * @param h the headers to modify
     * @param key the header key
     * @param value the header value
     */
    private static void putString(Headers h, String key, String value) {
        h.remove(key);
        h.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The acknowledgement callback of kafka-clients before 4.1, which is given
     * no headers and so cannot tell which trace the record belonged to. On those
     * clients hop spans come from {@link #onSend} instead, and this stays a
     * no-op. From 4.1 on, Kafka calls
     * {@link #onAcknowledgement(RecordMetadata, Exception, Headers)} instead.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // --- no-op
    }

    /**
     * Emits the hop's span once the broker has answered (kafka-clients 4.1+).
     *
     * <p>{@code headers} are those of the record as sent — after {@link #onSend}
     * — so they carry the {@code x-isotope} JSON whose last hop is this produce.
     * That is everything the span needs: no in-flight map is required to match
     * the acknowledgement back to its trace. On top of what {@code onSend} could
     * report, the span gains the partition and offset and, when {@code exception}
     * is set, is marked as a failed produce.
     *
     * <p>Kafka calls this on the producer's network I/O thread, including for
     * sends that fail before reaching the broker. So it only reads one header
     * and hands the bytes over; decoding happens on the span writer's thread.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception, Headers headers) {
        if (!spansOnAck || headers == null || !IsotopeSpans.isEnabled()) {
            return;
        }
        Header isotopeHeader = headers.lastHeader(Isotope.HEADER_KEY);
        if (isotopeHeader == null) {
            // onSend never stamped this record (it threw, or another interceptor
            // stripped the header): nothing to attribute a span to.
            return;
        }
        int partition = metadata == null ? -1 : metadata.partition();
        long offset = (metadata == null || !metadata.hasOffset()) ? -1L : metadata.offset();
        IsotopeSpans.recordAcknowledgedHopSpan(isotopeHeader.value(), partition, offset, exception);
    }

    private static boolean ackHeadersSupported() {
        try {
            ProducerInterceptor.class.getMethod(
                "onAcknowledgement", RecordMetadata.class, Exception.class, Headers.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /**
     * Hands back the span-sink reference taken in {@link #configure}. The OTLP
     * span writer's producer is shared across the JVM, so it is closed by
     * whichever interceptor releases the last reference, not by the first one to
     * close. No-op when spans were never enabled.
     */
    @Override
    public void close() {
        IsotopeSpans.release(spansAcquired);
        spansAcquired = false;
    }
}