# Isotope spans for OpenTelemetry (OTel)

Optional span writer for `kafka-isotope`. It turns every isotope hop into an **OpenTelemetry span** and writes it to a Kafka topic as an OTLP `ExportTraceServiceRequest` — the exact bytes a stock [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) reads.

Nothing isotope-specific runs on the consuming side. A Collector with a `kafka` receiver set to `encoding: otlp_proto` consumes the topic and exports to any OTLP backend (Groundcover, Jaeger, Tempo, Honeycomb…). The integration a customer has to perform is Collector config plus read access to one topic.

```
producer.send() ──▶ IsotopeProducerInterceptor ──▶ span queue ──▶ isotope_trace_spans
                            (hop appended)         (background)          │
                                                                         ▼
                                                    OTel Collector (kafka receiver)
                                                                         │
                                                                         ▼
                                                                  your OTLP backend
```

---

**Table of Contents**
<!-- toc -->
- [**1.0 Install**](#10-install)
- [**2.0 Put It To Work**](#20-put-it-to-work)
  + [**2.1 Collector Config**](#21-collector-config)
- [**3.0 What a Span Looks Like**](#30-what-a-span-looks-like)
- [**4.0 What It Costs a Send**](#40-what-it-costs-a-send)
  + [**4.1 It Has Its Own Producer**](#41-it-has-its-own-producer)
  + [**4.2 Lifecycle**](#42-lifecycle)
- [**5.0 Known gaps**](#50-known-gaps)
<!-- tocstop -->

---

## **1.0 Install**

```groovy
dependencies {
    implementation 'ai.signalroom:kafka-isotope-core:0.19.0'
    implementation 'ai.signalroom:kafka-isotope-otel:0.19.0' // optional — only for spans
}
```

## **2.0 Put It To Work**

Start the writer once at boot, after which the interceptor you already registered starts producing spans. Nothing else in your code changes.

```java
import ai.signalroom.kafka.isotope.otel.KafkaOtlpSpanSink;

Map<String, Object> spanProducerConfig = Map.of(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

KafkaOtlpSpanSink.start(spanProducerConfig);                       // isotope_trace_spans
KafkaOtlpSpanSink.start(spanProducerConfig, "my_spans_topic");     // or name your own
```

At shutdown, `KafkaOtlpSpanSink.close()` hands back the reference `start` took. The span producer itself goes away once every interceptor has closed too (see [§4.2 Lifecycle](#42-lifecycle)).

If the producer can't be built — bad `bootstrap.servers`, a security misconfiguration — the failure is logged at WARN and swallowed. Nothing is registered, spans stay off, and your pipeline runs on. Same posture as the Prometheus exporter: an optional sidecar never takes the application down with it.

t### **2.1 Collector Config**

```yaml
receivers:
  kafka:
    brokers: [kafka:9092]
    traces:
      topics: [isotope_trace_spans]
      encoding: otlp_proto
    group_id: otel-collector-isotope

exporters:
  otlp:
    endpoint: <your-otlp-endpoint>:4317

service:
  pipelines:
    traces:
      receivers: [kafka]
      exporters: [otlp]
```

> On Collector versions before the per-signal split, `topic` and `encoding` sit at
> the top level of the receiver instead of under `traces:`. The payload is the
> same either way.

## **3.0 What a Span Looks Like**

| OTel field | From |
|---|---|
| `trace_id` | the isotope trace id, verbatim — a 16-byte UUIDv7 *is* a 128-bit OTel trace id |
| `span_id` | derived: `sha256(trace_id, service, topic, ts)[0..8]` |
| `parent_span_id` | the same derivation over the previous entry in the hop list |
| `name` / `kind` | `send <topic>` / PRODUCER, or `receive <topic>` / CONSUMER |
| start → end | previous hop's timestamp → this hop's timestamp |
| `service.name` (resource) | the service on this edge |
| attributes | `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.operation.type`, `isotope.pipeline`, `isotope.origin_service`, `isotope.hop_count`, `isotope.truncated` |

**Derived span ids are the trick that makes this work without a wire-format change.** Hops carry no span id and never will — adding one would break every deployed reader. Instead each span's id is a hash of what the header already carries, so a service computes its parent's id from the previous entry in `h[]` and lands on exactly the id that parent computed for itself. Spans link into a tree across services with no shared state and nothing new on the wire.

The corollary: the hash is a compatibility surface. Change its inputs or encoding and spans from an older service stop matching parent ids computed by a newer one.

**Durations are real, not zero.** A hop is a single timestamp, so a span runs from the *previous* hop's timestamp to this one's, making each span the edge between two services rather than a point. Those two timestamps come from different machines, so a backwards clock is clamped to a zero-length span rather than a negative one.

## **4.0 What It Costs a Send**

Nothing that can be measured on the caller's thread, by construction:

- `onSend` copies a handful of fields into a small event and drops it on a bounded queue (10,000 by default). Hashing, protobuf encoding and the Kafka write all happen on a background daemon thread.
- Queue full ⇒ the span is dropped and counted (`KafkaOtlpSpanSink.droppedSpans()`), never queued behind a slow broker. Telemetry yields to the pipeline.
- Every error on the path is swallowed and logged, throttled to at most one warning a minute so a broker outage can't become a log flood.
- Span records are keyed by trace id, so one trace's spans stay on one partition and reach the Collector in order.

### **4.1 It Has Its Own Producer**

The writer owns a **separate producer**, shared by every interceptor in the JVM and built with `interceptor.classes` stripped, so its writes are never themselves traced. The interceptor cannot reuse the producer it is attached to: that one is mid-`send()`, and its interceptor chain would stamp the span records and trace the tracer. The consume-marker producer is separate for the same reason — the difference is that one is yours to manage and this one is ours.

Your config is used as given, except for what would make the producer trace itself or block: `interceptor.classes` is removed, the serializers are forced to `ByteArraySerializer`, and `acks=1`, `enable.idempotence=false`, `linger.ms=50`, `max.block.ms=1000` are applied *only if you didn't set them*.

### **4.2 Lifecycle**

`start()` takes the first reference and each configured interceptor takes one more. An interceptor's `close()` hands one back, `KafkaOtlpSpanSink.close()` hands back the starter's, and the producer shuts down when the last reference goes. This is why closing one producer can't pull the writer out from under its siblings — a real hazard for a JVM-wide resource that every interceptor shares. Shutdown drains whatever is still queued before closing.

## **5.0 Known gaps**

- **Spans record attempted produces.** They are emitted from `onSend`, not `onAcknowledgement`, because the acknowledgement callback receives neither the record nor its headers — matching an ack back to its trace needs a map of in-flight records keyed by correlation id. So a record the producer later drops still has a span, and spans carry no broker-assigned partition or offset. That map is the next version's problem.
- **Consume spans need the full header.** `IsotopeContext.recordConsume` emits a consume span only when the record still carries the `x-isotope` JSON, since the span's parent comes from the hop list. Records carrying only the scalar headers — including the consume markers themselves — produce no span, which is what we want: markers are report input, not pipeline edges.
- **No span events or links**, and no status beyond UNSET. There is nothing in an isotope header to populate them with.