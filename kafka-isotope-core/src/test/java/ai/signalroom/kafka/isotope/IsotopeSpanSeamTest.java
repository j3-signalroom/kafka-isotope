/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the span seam in core — that {@link IsotopeProducerInterceptor}
 * and {@link IsotopeContext} emit through {@link IsotopeSpans}, that the trace
 * they hand a sink carries the parent edge, and that a sink can neither be
 * released out from under its siblings nor break a {@code send()}.
 */
class IsotopeSpanSeamTest {

    private static final String SERVICE = "order-intake-service";
    private static final String PIPELINE = "orders";
    private static final String TOPIC_A = "topic-a";
    private static final String TOPIC_B = "topic-b";

    /** Records what core hands the sink, so the tests can inspect it. */
    private static final class CapturingSpanSink implements IsotopeSpanSink {

        record HopSpan(Isotope isotope, String service, String topic, long tsMs) {}

        record ConsumeSpan(Isotope isotope, String service, String topic, long tsMs) {}

        record AckedSpan(byte[] isotopeJson, int partition, long offset, Exception error) {}

        final List<HopSpan> hopSpans = new ArrayList<>();
        final List<AckedSpan> ackedSpans = new ArrayList<>();
        final List<ConsumeSpan> consumeSpans = new ArrayList<>();
        final AtomicInteger refs = new AtomicInteger();

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void recordHopSpan(Isotope isotope, String thisService, String thisTopic, long hopTsMs) {
            hopSpans.add(new HopSpan(isotope, thisService, thisTopic, hopTsMs));
        }

        @Override
        public void recordAcknowledgedHopSpan(byte[] isotopeJson, int partition,
                long offset, Exception error) {
            ackedSpans.add(new AckedSpan(isotopeJson, partition, offset, error));
        }

        @Override
        public void recordConsumeSpan(Isotope isotope, String consumerService,
                String consumedTopic, long consumeTsMs) {
            consumeSpans.add(new ConsumeSpan(isotope, consumerService, consumedTopic, consumeTsMs));
        }

        @Override
        public void acquire() {
            refs.incrementAndGet();
        }

        @Override
        public void release() {
            refs.decrementAndGet();
        }
    }

    @AfterEach
    void reset() {
        IsotopeSpans.reset();
        IsotopeContext.clear();
    }

    /** What a 4.1+ producer reports for a record the broker accepted. */
    private static RecordMetadata written(String topic, int partition, long offset) {
        return new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 0L, 0, 0);
    }

    private static IsotopeProducerInterceptor<byte[], byte[]> interceptor() {
        IsotopeProducerInterceptor<byte[], byte[]> i = new IsotopeProducerInterceptor<>();
        i.configure(Map.of(
            IsotopeProducerInterceptor.SERVICE_NAME_CONFIG, SERVICE,
            IsotopeProducerInterceptor.PIPELINE_NAME_CONFIG, PIPELINE));
        return i;
    }

    private static ProducerRecord<byte[], byte[]> record(String topic) {
        return new ProducerRecord<>(topic, null, null);
    }

    /** Turns a just-produced record into the consumer record a next stage would see. */
    private static ConsumerRecord<byte[], byte[]> asConsumed(ProducerRecord<byte[], byte[]> produced) {
        RecordHeaders headers = new RecordHeaders();
        produced.headers().forEach(h -> headers.add(h.key(), h.value()));
        return new ConsumerRecord<>(
            produced.topic(), 0, 7L, 0L, null, 0, 0, null, null, headers, java.util.Optional.empty());
    }

    @Test
    void spansAreOffUntilASinkRegisters() {
        assertFalse(IsotopeSpans.isEnabled(), "no sink registered yet");
        assertFalse(IsotopeSpans.acquire(), "the no-op sink takes no references");

        // The interceptor still works with spans off.
        ProducerRecord<byte[], byte[]> out = interceptor().onSend(record(TOPIC_A));
        assertNotNull(out.headers().lastHeader(Isotope.HEADER_KEY));
    }

    @Test
    void thisClasspathTakesTheAcknowledgementPath() {
        // Guards the tests below: on kafka-clients 4.1+ the headers overload
        // exists, so without this they could silently test only the fallback.
        assertTrue(IsotopeProducerInterceptor.ACK_HEADERS_SUPPORTED);
        assertTrue(interceptor().spansOnAck);
    }

    @Test
    void theHopSpanWaitsForTheAcknowledgement() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);
        IsotopeProducerInterceptor<byte[], byte[]> producing = interceptor();

        ProducerRecord<byte[], byte[]> sent = producing.onSend(record(TOPIC_A));
        assertTrue(sink.hopSpans.isEmpty(), "nothing is reported until the broker answers");
        assertTrue(sink.ackedSpans.isEmpty());

        producing.onAcknowledgement(written(TOPIC_A, 3, 42L), null, sent.headers());

        assertEquals(1, sink.ackedSpans.size(), "one span per hop, from the acknowledgement");
        assertTrue(sink.hopSpans.isEmpty(), "and never a second one from onSend");
        CapturingSpanSink.AckedSpan acked = sink.ackedSpans.get(0);
        assertEquals(3, acked.partition());
        assertEquals(42L, acked.offset());
        assertNull(acked.error());

        // The bytes handed over are the x-isotope header as sent: the hop just
        // appended is last, which is what lets the sink derive the span.
        Isotope asSent = Isotope.fromJsonBytes(acked.isotopeJson());
        assertEquals(1, asSent.hops().size());
        assertEquals(TOPIC_A, asSent.hops().get(0).topic());
        assertEquals(SERVICE, asSent.hops().get(0).service());
    }

    @Test
    void aFailedSendIsReportedWithItsError() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);
        IsotopeProducerInterceptor<byte[], byte[]> producing = interceptor();
        ProducerRecord<byte[], byte[]> sent = producing.onSend(record(TOPIC_A));

        // What Kafka reports when a send fails before a partition is assigned.
        TimeoutException failure = new TimeoutException("metadata not available");
        producing.onAcknowledgement(
            new RecordMetadata(new TopicPartition(TOPIC_A, RecordMetadata.UNKNOWN_PARTITION), -1L, -1, 0L, -1, -1),
            failure, sent.headers());

        CapturingSpanSink.AckedSpan acked = sink.ackedSpans.get(0);
        assertSame(failure, acked.error());
        assertEquals(-1, acked.partition());
        assertEquals(-1L, acked.offset(), "no offset: the record was never written");
    }

    @Test
    void anAcknowledgementWithoutAnIsotopeHeaderIsIgnored() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);

        // e.g. onSend threw and Kafka sent the record unstamped.
        interceptor().onAcknowledgement(written(TOPIC_A, 0, 1L), null, new RecordHeaders());
        interceptor().onAcknowledgement(written(TOPIC_A, 0, 1L), null, null);

        assertTrue(sink.ackedSpans.isEmpty());
    }

    @Test
    void theDefaultAcknowledgementDelegatesToTheSendTimeMethod() {
        // A sink that only implements the send-time method must still see every
        // hop on a 4.1+ client, via the interface's default.
        List<CapturingSpanSink.HopSpan> seen = new ArrayList<>();
        IsotopeSpans.register(new IsotopeSpanSink() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void recordHopSpan(Isotope isotope, String thisService,
                    String thisTopic, long hopTsMs) {
                seen.add(new CapturingSpanSink.HopSpan(isotope, thisService, thisTopic, hopTsMs));
            }

            @Override
            public void recordConsumeSpan(Isotope isotope, String consumerService,
                    String consumedTopic, long consumeTsMs) {
                // --- not under test
            }
        });

        IsotopeProducerInterceptor<byte[], byte[]> producing = interceptor();
        ProducerRecord<byte[], byte[]> sent = producing.onSend(record(TOPIC_A));
        producing.onAcknowledgement(written(TOPIC_A, 0, 5L), null, sent.headers());

        assertEquals(1, seen.size());
        assertEquals(SERVICE, seen.get(0).service());
        assertEquals(TOPIC_A, seen.get(0).topic());
        assertEquals(seen.get(0).isotope().hops().get(0).tsMs(), seen.get(0).tsMs());
    }

    @Test
    void preFourOneClientsFallBackToOnSendWithTheParentEdge() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);

        // Stage one: origin produce, on a client with no headers in the ack.
        IsotopeProducerInterceptor<byte[], byte[]> stageOne = interceptor();
        stageOne.spansOnAck = false;
        ProducerRecord<byte[], byte[]> first = stageOne.onSend(record(TOPIC_A));

        assertEquals(1, sink.hopSpans.size(), "emitted at send time instead");
        CapturingSpanSink.HopSpan origin = sink.hopSpans.get(0);
        assertEquals(SERVICE, origin.service());
        assertEquals(TOPIC_A, origin.topic());
        assertEquals(1, origin.isotope().hops().size(), "origin hop has no predecessor");
        assertEquals(origin.tsMs(), origin.isotope().hops().get(0).tsMs(),
            "the timestamp handed to the sink is the hop's own");

        // An old client would never call this; if it somehow did, no double span.
        stageOne.onAcknowledgement(written(TOPIC_A, 0, 1L), null, first.headers());
        assertTrue(sink.ackedSpans.isEmpty());

        // Stage two: adopt the trace and re-produce onto another topic.
        IsotopeContext.adoptFromRecord(asConsumed(first));
        IsotopeProducerInterceptor<byte[], byte[]> stageTwo = interceptor();
        stageTwo.spansOnAck = false;
        stageTwo.onSend(record(TOPIC_B));

        assertEquals(2, sink.hopSpans.size());
        CapturingSpanSink.HopSpan second = sink.hopSpans.get(1);
        List<Isotope.Hop> hops = second.isotope().hops();
        assertEquals(2, hops.size());
        assertEquals(TOPIC_A, hops.get(0).topic(), "the entry before this hop is the parent edge");
        assertEquals(TOPIC_B, hops.get(1).topic(), "this hop is last");
        assertEquals(second.tsMs(), hops.get(1).tsMs());
    }

    @Test
    void recordConsumeRecordsAConsumeSpanParentedToTheLastHop() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);

        ProducerRecord<byte[], byte[]> produced = interceptor().onSend(record(TOPIC_A));
        MockProducer<byte[], byte[]> markers = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());

        IsotopeContext.recordConsume(asConsumed(produced), "shipping-notification-service", markers);

        assertEquals(1, markers.history().size(), "the marker is still written");
        assertEquals(1, sink.consumeSpans.size());
        CapturingSpanSink.ConsumeSpan consume = sink.consumeSpans.get(0);
        assertEquals("shipping-notification-service", consume.service());
        assertEquals(TOPIC_A, consume.topic(), "the span names the topic consumed from");
        assertEquals(1, consume.isotope().hops().size());
        assertEquals(TOPIC_A, consume.isotope().hops().get(0).topic(),
            "the last hop is the produce this consume answers");
    }

    @Test
    void scalarOnlyRecordsGetAMarkerButNoSpan() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);

        // A marker record: the seven scalars, no x-isotope JSON.
        RecordHeaders headers = new RecordHeaders();
        headers.add(Isotope.HEADER_TRACE_ID, "0192abcd".getBytes(StandardCharsets.UTF_8));
        headers.add(Isotope.HEADER_ORIGIN_TS, "1700000000000".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<byte[], byte[]> scalarsOnly = new ConsumerRecord<>(
            TOPIC_A, 0, 1L, 0L, null, 0, 0, null, null, headers, java.util.Optional.empty());

        MockProducer<byte[], byte[]> markers = new MockProducer<>(
            true, null, new ByteArraySerializer(), new ByteArraySerializer());
        IsotopeContext.recordConsume(scalarsOnly, "auditor", markers);

        assertEquals(1, markers.history().size(), "marker still emitted");
        assertTrue(sink.consumeSpans.isEmpty(), "no hop list, no span");
    }

    @Test
    void interceptorReleasesExactlyTheReferenceItTook() {
        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);

        IsotopeProducerInterceptor<byte[], byte[]> one = interceptor();
        IsotopeProducerInterceptor<byte[], byte[]> two = interceptor();
        assertEquals(2, sink.refs.get(), "one reference per configured interceptor");

        one.close();
        assertEquals(1, sink.refs.get(), "one interceptor closing leaves the other's reference");
        two.close();
        assertEquals(0, sink.refs.get());

        // Closing twice must not hand back a reference it no longer holds.
        two.close();
        assertEquals(0, sink.refs.get());
    }

    @Test
    void anInterceptorConfiguredBeforeTheSinkNeverReleasesIt() {
        // Spans off at configure() time: the interceptor took no reference, so
        // its close() must not decrement a sink that registered in between.
        IsotopeProducerInterceptor<byte[], byte[]> early = interceptor();

        CapturingSpanSink sink = new CapturingSpanSink();
        IsotopeSpans.register(sink);
        sink.acquire(); // stands in for the writer's own starter reference

        early.close();
        assertEquals(1, sink.refs.get(), "the starter's reference survives");
    }

    @Test
    void aThrowingSinkCannotBreakSend() {
        IsotopeSpans.register(new IsotopeSpanSink() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public void recordHopSpan(Isotope isotope, String thisService,
                    String thisTopic, long hopTsMs) {
                throw new IllegalStateException("span backend on fire");
            }

            @Override
            public void recordConsumeSpan(Isotope isotope, String consumerService,
                    String consumedTopic, long consumeTsMs) {
                throw new IllegalStateException("span backend on fire");
            }
        });

        // The send-time path (pre-4.1 clients)...
        IsotopeProducerInterceptor<byte[], byte[]> legacy = interceptor();
        legacy.spansOnAck = false;
        ProducerRecord<byte[], byte[]> in = record(TOPIC_A);
        ProducerRecord<byte[], byte[]> out = legacy.onSend(in);

        assertSame(in, out, "send() sails on");
        assertNotNull(out.headers().lastHeader(Isotope.HEADER_KEY), "trace propagation unaffected");

        // ...and the acknowledgement path, whose default decodes then calls the
        // same throwing method.
        IsotopeProducerInterceptor<byte[], byte[]> current = interceptor();
        ProducerRecord<byte[], byte[]> sent = current.onSend(record(TOPIC_A));
        assertDoesNotThrow(() -> current.onAcknowledgement(written(TOPIC_A, 0, 1L), null, sent.headers()));
    }
}
