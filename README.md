# kafka-isotope

End-to-end **record tracing for Apache Kafka**, built entirely on public extension points — a `ProducerInterceptor` plus record headers. No broker changes, no agent, no vendor lock-in. Stamp a UUIDv7 trace id and origin metadata on first produce, accumulate a hop on every re-produce, and reconstruct latency / topology / hop-distribution from the headers (or from the optional Prometheus meters).

## **1.0 Modules**

| Module | Coordinate | Pulls | Use it for |
|---|---|---|---|
| **kafka-isotope-core** | `ai.signalroom:kafka-isotope-core` | Jackson, SLF4J (`kafka-clients` is `compileOnly`) | Trace **propagation** — interceptor, headers, consume markers. |
| **kafka-isotope-metrics** | `ai.signalroom:kafka-isotope-metrics` | kafka-isotope-core + Micrometer/Prometheus | Optional `/metrics` exporter for the stateless reports. |

`kafka-isotope-core` never depends on a metrics library: emission routes through a no-op `IsotopeMetricsSink` until `kafka-isotope-metrics` registers the Prometheus one, so propagation runs with zero metrics overhead.

## **2.0 Install using Gradle**

```groovy
repositories {
    maven { url 'https://maven.pkg.github.com/j3-signalroom/kafka-isotope' }
}
dependencies {
    implementation 'ai.signalroom:kafka-isotope-core:0.18.0'
    implementation 'ai.signalroom:kafka-isotope-metrics:0.18.0' // optional — only for Prometheus
}
```

See **[kafka-isotope-core/README.md](kafka-isotope-core/README.md)** for the full adopter quickstart (registering the interceptor, adopting/marking on consume, starting the exporter).

## Demo

A runnable reference pipeline (Confluent Platform / Confluent Cloud on Minikube, the seven Flink reports, and a one-command Prometheus + Grafana showcase) lives in the companion repo:
[j3-signalroom/confluent-kafka-isotope](https://github.com/j3-signalroom/confluent-kafka-isotope).
