# Isotope metrics for Prometheus

Optional Prometheus exporter for `kafka-isotope`. It turns the values the interceptor and consume helpers already have in hand into Micrometer meters and serves them at `GET /metrics`, a metrics-native alternative to three of the Flink reports (`latency_1m`, `topology_1m`, `hop_distribution_1m`).

Nothing new runs in your pipeline. Every value comes from what `IsotopeProducerInterceptor` and `IsotopeContext` compute anyway, and Prometheus does the 1-minute windowing at query time with `rate()` / `increase()`.

---

**Table of Contents**
<!-- toc -->
- [**1.0 Install**](#10-install)
- [**2.0 Put It To Work**](#20-put-it-to-work)
  + [**2.1 Prometheus Scrape Config**](#21-prometheus-scrape-config)
- [**3.0 What It Emits**](#30-what-it-emits)
- [**4.0 Why Three of Seven Reports**](#40-why-three-of-seven-reports)
- [**5.0 Known Gaps vs. The Flink Reports**](#50-known-gaps-vs-the-flink-reports)
- [**6.0 How It Wires In**](#60-how-it-wires-in)
<!-- tocstop -->

---

## **1.0 Install**

```groovy
dependencies {
    implementation 'ai.signalroom:kafka-isotope-core:0.19.0'
    implementation 'ai.signalroom:kafka-isotope-metrics:0.19.0' // optional — only for Prometheus
}
```

## **2.0 Put It To Work**

Start the exporter once at boot, after which the interceptor you already registered starts recording. Nothing else in your code changes.

```java
import ai.signalroom.kafka.isotope.metrics.PrometheusIsotopeMetrics;

PrometheusIsotopeMetrics.start(9404); // serves http://0.0.0.0:9404/metrics
```

`start` is idempotent: only the first call opens the listener.

If the port is taken (commonly a sibling stage already holding 9404 on the same host), the failure is logged at WARN and swallowed. Nothing is registered, the metric calls stay inert, and your pipeline runs on. An optional sidecar never takes the application down with it. Give each stage on a host its own port.

### **2.1 Prometheus Scrape Config**

```yaml
scrape_configs:
  - job_name: kafka-isotope
    static_configs:
      - targets: ['order-intake-service:9404', 'order-enrichment-service:9404']
```

## **3.0 What It Emits**

Micrometer's Prometheus registry publishes timers in seconds, as `_count`, `_sum` and `_max` series, and counters as `_total`.

**Produce side**, from `IsotopeProducerInterceptor.onSend`:

| Metric | Tags | Gives you |
|---|---|---|
| `isotope_hop_latency_seconds_*` | `pipeline`, `origin_service`, `this_service`, `this_topic` | Origin → this-hop latency (avg from `sum/count`, `max`). Its `count` doubles as the produce-edge record count for topology. |
| `isotope_hop_records_total` | `pipeline`, `this_topic`, `hop_count` | Hop distribution: how many records reach each topic at each depth. |

**Consume side**, from `IsotopeContext`:

| Metric | Tags | Emitted by | Gives you |
|---|---|---|---|
| `isotope_consume_latency_seconds_*` | `pipeline`, `origin_service`, `consumer_service`, `this_topic` | `recordConsume` | Origin → consume time, measured when the consume marker is written. |
| `isotope_consume_records_total` | `pipeline`, `this_topic`, `consumer_service` | `recordConsume` | Topic → consumer edge counts, the consume half of the bipartite topology. |
| `isotope_consume_age_seconds_*` | `pipeline`, `origin_service`, `consumer_service`, `this_topic` | `adoptFromRecord(record, service)`, or `recordConsume` for terminal consumers | How stale a record was when consumed. Emitted exactly once per consuming stage. |

The two consume paths never double-count age. A stage that adopts the trace emits it on adoption, and `recordConsume` emits it only when nothing was adopted on that thread (a terminal consumer). Use the two-argument `adoptFromRecord(record, consumerService)` to get the adoption-side sample; the one-argument form records nothing.

For the PromQL behind each report and the Grafana dashboards, see [docs/metrics.md](https://github.com/j3-signalroom/confluent-kafka-isotope/blob/main/docs/metrics.md) in the companion repo.

## **4.0 Why Three of Seven Reports**

These three are pure aggregation over dimensions with a bounded number of values: service, topic, pipeline and `hop_count` (itself capped at `Isotope.MAX_HOPS`). The interceptor already has every value in scope on each `send()`, so no stream processor is needed.

The other four (merged percentiles, coverage, bipartite topology and stuck traces) need per-`trace_id` state or have to detect an event that *didn't* happen. Prometheus can express neither, so they stay in Flink. The design reasoning is in [docs/design.md](https://github.com/j3-signalroom/confluent-kafka-isotope/blob/main/docs/design.md).

## **5.0 Known Gaps vs. The Flink Reports**

- **No `distinct_traces`.** A counter can't deduplicate, and `trace_id` is never used as a tag, because it has unbounded cardinality and would blow up the series count.
- **No windowed `min` latency.** A Micrometer timer exposes `max` but not a per-window minimum.

## **6.0 How It Wires In**

`kafka-isotope-core` has no metrics dependency. Its emissions route through the `IsotopeMetrics` facade to a no-op sink (`NoOpMetricsSink`) until `start` registers `PrometheusIsotopeMetrics` in its place. Having this module on the classpath registers nothing by itself. Until you call `start`, each send costs an "is it enabled?" check and no metric work. The exporter keeps its own Prometheus registry rather than the process-global one, so the isotope meters are the only thing on its `/metrics` endpoint.