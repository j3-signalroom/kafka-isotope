# `kafka-isotope` Library

`kafka-isotope` provides **end-to-end record tracing for Apache Kafka** using Kafka record headers, a `ProducerInterceptor`, and optional Prometheus metrics.

It works by attaching lightweight tracing artifacts—called *isotopes*—to records as they move through Kafka pipelines.

An isotope is a small tracing payload carried in Kafka record headers. Like a biochemical isotope used to trace molecules through a metabolic pathway, it enables the journey of a record through an event-driven architecture to be observed and analyzed.

On first produce, `kafka-isotope` stamps a UUIDv7 trace ID and origin metadata onto the record. Each subsequent re-produce appends another hop, allowing the full path of a record through Kafka pipelines to be reconstructed.

Built entirely on Kafka’s public extension points—a `ProducerInterceptor` plus record headers—`kafka-isotope` requires:

* No broker changes
* No sidecar agents
* No vendor lock-in

From isotope headers, `kafka-isotope` derives trace data that can be analyzed directly, exported as Prometheus metrics, or emitted as OpenTelemetry spans to support:

* End-to-end latency
* Pipeline topology
* Hop distribution
* Drift detection
* Stuck trace detection
* Per-trace forensic replay

This makes `kafka-isotope` a lightweight but powerful observability layer for Kafka-based event-driven architectures.

---

**Table of Contents**
<!-- toc -->
- [**1.0 Modules**](#10-modules)
- [**2.0 Install using Gradle**](#20-install-using-gradle)
- [**3.0 Demo**](#30-demo)
<!-- tocstop -->

---

## **1.0 Modules**

| Module | Coordinate | Pulls | Use it for |
|---|---|---|---|
| **kafka-isotope-core** | `ai.signalroom:kafka-isotope-core` | Jackson, SLF4J (`kafka-clients` is `compileOnly`) | Trace **propagation** — interceptor, headers, consume markers. |
| **kafka-isotope-metrics** | `ai.signalroom:kafka-isotope-metrics` | kafka-isotope-core + Micrometer/Prometheus | Optional `/metrics` exporter for the stateless reports. |
| **kafka-isotope-otel** | `ai.signalroom:kafka-isotope-otel` | kafka-isotope-core + OTLP protobuf | Optional **span** writer — one OTLP span per hop on a Kafka topic, read by a stock OpenTelemetry Collector. |

`kafka-isotope-core` has no metrics or tracing dependency. It routes every emission through two sinks that start out as no-ops (`NoOpMetricsSink` and `NoOpSpanSink`). Adding `kafka-isotope-metrics` or `kafka-isotope-otel` to the classpath doesn't change that: the real sink is registered only when your application calls `PrometheusIsotopeMetrics.start(port)` or `KafkaOtlpSpanSink.start(config)`, and stays a no-op if that call fails. Until then, each record costs an "is it enabled?" check and no metric or span work.

## **2.0 Install using Gradle**

```groovy
repositories {
    mavenCentral()
}
dependencies {
    implementation 'ai.signalroom:kafka-isotope-core:0.19.0'
    implementation 'ai.signalroom:kafka-isotope-metrics:0.19.0' // optional — only for Prometheus metrics
    implementation 'ai.signalroom:kafka-isotope-otel:0.19.0'    // optional — only for OTel spans
}
```

See **[kafka-isotope-core/README.md](kafka-isotope-core/README.md)** for the full adopter quickstart (registering the interceptor, adopting/marking on consume, starting the exporter). The optional modules have their own: **[kafka-isotope-metrics/README.md](kafka-isotope-metrics/README.md)** for the Prometheus metrics, and **[kafka-isotope-otel/README.md](kafka-isotope-otel/README.md)** for the span writer and the Collector config that reads it.

## **3.0 Demo**

A runnable reference pipeline (Confluent Platform / Confluent Cloud on Minikube, the seven Flink reports, and a one-command Prometheus + Grafana showcase) lives in the companion repo:
[j3-signalroom/confluent-kafka-isotope](https://github.com/j3-signalroom/confluent-kafka-isotope).