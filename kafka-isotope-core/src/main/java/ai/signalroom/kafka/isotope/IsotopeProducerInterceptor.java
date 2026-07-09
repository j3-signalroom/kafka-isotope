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
     * This method is called once per record acknowledged by the broker. In this interceptor, it is a no-op because
     * the hop is appended in onSend, and the broker-assigned partition and offset are not threaded back into the
     * header. If you need to capture broker-assigned metadata, you would need to implement a custom callback and
     * manage the in-flight isotopes in a way that allows you to correlate the onAcknowledgement call with the
     * original record and its isotope. For the purposes of this interceptor, we rely on the fact that the hop
     * information is already captured in the onSend method, and we do not need to modify the isotope or headers upon
     * acknowledgment. If you want to extend this interceptor to capture acknowledgment metadata, you would need to
     * design a mechanism to store the in-flight isotopes in a concurrent map keyed by some correlation ID, and then
     * update the isotope with the acknowledgment metadata in this method. However, that is beyond the scope of this
     * basic interceptor implementation.
     * 
     * In summary, the onAcknowledgement method is intentionally left as a no-op in this interceptor because the necessary
     * hop information is already captured in the onSend method, and we do not have a mechanism to correlate acknowledgments
     * with their corresponding isotopes in this implementation.
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // --- no-op
    }

    @Override
    public void close() {
        // --- no-op
    }
}