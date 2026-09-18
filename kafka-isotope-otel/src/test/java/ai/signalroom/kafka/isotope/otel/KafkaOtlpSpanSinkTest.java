/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.Span;

import ai.signalroom.kafka.isotope.Isotope;
import ai.signalroom.kafka.isotope.IsotopeContext;
import ai.signalroom.kafka.isotope.IsotopeProducerInterceptor;
import ai.signalroom.kafka.isotope.IsotopeSpans;

/**
 * Unit tests for {@link KafkaOtlpSpanSink} driven the way production drives it —
 * through {@link IsotopeProducerInterceptor} and
 * {@link IsotopeContext#recordConsume} — with a {@link MockProducer} standing in
 * for the span producer, so no broker is needed.
 */
class KafkaOtlpSpanSinkTest {

    private static final String SERVICE = "order-intake-service";
    private static final String PIPELINE = "orders";
    private static final String TOPIC_A = "topic-a";
    private static final String SPANS_TOPIC = KafkaOtlpSpanSink.DEFAULT_TOPIC;
    private static final long WAIT_MS = 5_000L;

    /** A MockProducer that also hands sent records to the test thread. */
    private static class RecordingProducer extends MockProducer<byte[], byte[]> {

        final LinkedBlockingQueue<ProducerRecord<byte[], byte[]>> sent = new LinkedBlockingQueue<>();

        RecordingProducer() {
            super(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            sent.add(record);
            return super.send(record, callback);
        }
    }

    /** A producer whose first send parks, so the queue behind it can fill up. */
    private static final class StalledProducer extends RecordingProducer {

        final CountDownLatch inSend = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            inSend.countDown();
            try {
                release.await(WAIT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.send(record, callback);
        }
    }

    @AfterEach
    void reset() {
        KafkaOtlpSpanSink.resetForTest();
        IsotopeContext.clear();
    }

    private static IsotopeProducerInterceptor<byte[], byte[]> interceptor() {
        IsotopeProducerInterceptor<byte[], byte[]> i = new IsotopeProducerInterceptor<>();
        i.configure(Map.of(
            IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, SERVICE,
            IsotopeProducerInterceptor.PIPELINE_NAME_CONFIG, PIPELINE));
        return i;
    }

    private static ConsumerRecord<byte[], byte[]> asConsumed(ProducerRecord<byte[], byte[]> produced) {
        RecordHeaders headers = new RecordHeaders();
        produced.headers().forEach(h -> headers.add(h.key(), h.value()));
        return new ConsumerRecord<>(
            produced.topic(), 0, 7L, 0L, null, 0, 0, null, null, headers, Optional.empty());
    }

    /** Waits for at least {@code want} spans to be written, returning all of them. */
    private static List<Span> awaitSpans(RecordingProducer producer, int want) throws InterruptedException {
        List<Span> spans = new ArrayList<>();
        long deadline = System.currentTimeMillis() + WAIT_MS;
        while (spans.size() < want && System.currentTimeMillis() < deadline) {
            ProducerRecord<byte[], byte[]> record =
                producer.sent.poll(200, TimeUnit.MILLISECONDS);
            if (record == null) {
                continue;
            }
            assertEquals(SPANS_TOPIC, record.topic());
            try {
                ExportTraceServiceRequest request =
                    ExportTraceServiceRequest.parseFrom(record.value());
                request.getResourceSpansList().forEach(rs ->
                    rs.getScopeSpansList().forEach(ss -> spans.addAll(ss.getSpansList())));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new AssertionError("span record was not a valid OTLP request", e);
            }
        }
        return spans;
    }

    @Test
    void producerConfigNeverTracesItselfAndAlwaysWritesBytes() {
        Map<String, Object> config = KafkaOtlpSpanSink.spanProducerConfig(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, IsotopeProducerInterceptor.class.getName(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.LINGER_MS_CONFIG, 999));

        assertFalse(config.containsKey(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
            "the span producer must not run the interceptor that feeds it");
        assertEquals(ByteArraySerializer.class.getName(),
            config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(ByteArraySerializer.class.getName(),
            config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("localhost:9092", config.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(999, config.get(ProducerConfig.LINGER_MS_CONFIG),
            "an explicit setting is the caller's to make");
        assertEquals(Boolean.FALSE, config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("1", config.get(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    @Timeout(30)
    void aProducedHopBecomesAnOtlpSpanOnTheSpansTopic() throws InterruptedException {
        RecordingProducer spanProducer = new RecordingProducer();
        KafkaOtlpSpanSink.startWithProducer(spanProducer, SPANS_TOPIC, 100);
        assertTrue(IsotopeSpans.isEnabled(), "starting registers the sink into core");

        ProducerRecord<byte[], byte[]> produced = interceptor().onSend(
            new ProducerRecord<>(TOPIC_A, null, null));
        Isotope trace = Isotope.fromHeaders(produced.headers());

        List<Span> spans = awaitSpans(spanProducer, 1);
        assertEquals(1, spans.size());
        Span span = spans.get(0);
        assertEquals(Span.SpanKind.SPAN_KIND_PRODUCER, span.getKind());
        assertEquals("send " + TOPIC_A, span.getName());
        assertArrayEquals(trace.traceId(), span.getTraceId().toByteArray());

        ProducerRecord<byte[], byte[]> spanRecord = spanProducer.history().get(0);
        assertArrayEquals(trace.traceId(), spanRecord.key(),
            "records are keyed by trace id, so a trace's spans stay on one partition");
        assertEquals(0, KafkaOtlpSpanSink.droppedSpans());
        assertEquals(0, KafkaOtlpSpanSink.failedSpanSends());
    }

    @Test
    @Timeout(30)
    void aConsumeEdgeBecomesAChildOfTheProduceSpan() throws InterruptedException {
        RecordingProducer spanProducer = new RecordingProducer();
        KafkaOtlpSpanSink.startWithProducer(spanProducer, SPANS_TOPIC, 100);

        ProducerRecord<byte[], byte[]> produced = interceptor().onSend(
            new ProducerRecord<>(TOPIC_A, null, null));
        MockProducer<byte[], byte[]> markers = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());
        IsotopeContext.recordConsume(asConsumed(produced), "shipping-notification-service", markers);

        List<Span> spans = awaitSpans(spanProducer, 2);
        assertEquals(2, spans.size());
        Span produce = spans.stream()
            .filter(s -> s.getKind() == Span.SpanKind.SPAN_KIND_PRODUCER).findFirst().orElseThrow();
        Span consume = spans.stream()
            .filter(s -> s.getKind() == Span.SpanKind.SPAN_KIND_CONSUMER).findFirst().orElseThrow();

        assertEquals(produce.getTraceId(), consume.getTraceId(), "one trace, end to end");
        assertArrayEquals(produce.getSpanId().toByteArray(), consume.getParentSpanId().toByteArray(),
            "the consumer's span hangs off the produce span, across two services");
    }

    @Test
    @Timeout(30)
    void spansAreDroppedRatherThanMadeToWait() throws InterruptedException {
        StalledProducer spanProducer = new StalledProducer();
        KafkaOtlpSpanSink.startWithProducer(spanProducer, SPANS_TOPIC, 2);

        IsotopeProducerInterceptor<byte[], byte[]> producing = interceptor();

        // The first span takes the writer thread into send(), where it parks.
        producing.onSend(new ProducerRecord<>(TOPIC_A, null, null));
        assertTrue(spanProducer.inSend.await(WAIT_MS, TimeUnit.MILLISECONDS),
            "the writer should have reached the producer");

        // With the writer stuck, the queue (capacity 2) fills and the rest go
        // over the side — the sends themselves must not notice.
        long start = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            IsotopeContext.clear();
            producing.onSend(new ProducerRecord<>(TOPIC_A, null, null));
        }
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(3, KafkaOtlpSpanSink.droppedSpans(),
            "two fit in the queue, three are dropped");
        assertTrue(elapsed < 1_000, "sends did not wait on the stalled writer (took " + elapsed + "ms)");

        spanProducer.release.countDown();
    }

    @Test
    @Timeout(30)
    void oneInterceptorClosingDoesNotTakeTheSharedWriterWithIt() {
        KafkaOtlpSpanSink.startWithProducer(new RecordingProducer(), SPANS_TOPIC, 100);

        IsotopeProducerInterceptor<byte[], byte[]> one = interceptor();
        IsotopeProducerInterceptor<byte[], byte[]> two = interceptor();

        one.close();
        assertTrue(KafkaOtlpSpanSink.INSTANCE.isEnabled(),
            "the writer is shared; one producer closing must not stop it");
        two.close();
        assertTrue(KafkaOtlpSpanSink.INSTANCE.isEnabled(),
            "the starter still holds its reference");

        KafkaOtlpSpanSink.close();
        assertFalse(KafkaOtlpSpanSink.INSTANCE.isEnabled(), "last reference out shuts it down");
        assertFalse(IsotopeSpans.isEnabled(), "and core is back on the no-op sink");
    }

    @Test
    @Timeout(30)
    void theLastInterceptorOutShutsTheWriterDown() {
        KafkaOtlpSpanSink.startWithProducer(new RecordingProducer(), SPANS_TOPIC, 100);
        IsotopeProducerInterceptor<byte[], byte[]> only = interceptor();

        // The application closes first this time; the interceptor outlives it.
        KafkaOtlpSpanSink.close();
        assertTrue(KafkaOtlpSpanSink.INSTANCE.isEnabled(),
            "an interceptor is still producing — keep writing its spans");

        only.close();
        assertFalse(KafkaOtlpSpanSink.INSTANCE.isEnabled());
        assertFalse(IsotopeSpans.isEnabled());
    }

    @Test
    @Timeout(30)
    void queuedSpansAreFlushedOnShutdown() throws InterruptedException {
        RecordingProducer spanProducer = new RecordingProducer();
        KafkaOtlpSpanSink.startWithProducer(spanProducer, SPANS_TOPIC, 100);

        interceptor().onSend(new ProducerRecord<>(TOPIC_A, null, null));
        KafkaOtlpSpanSink.close();

        assertFalse(awaitSpans(spanProducer, 1).isEmpty(),
            "shutdown drains what was still queued");
    }
}
