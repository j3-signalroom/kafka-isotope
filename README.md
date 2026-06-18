# kafka-isotope

End-to-end **record tracing for Apache Kafka**, built entirely on public
extension points — a `ProducerInterceptor` plus record headers. No broker
changes, no agent, no vendor lock-in. Stamp a UUIDv7 trace id and origin
metadata on first produce, accumulate a hop on every re-produce, and
reconstruct latency / topology / hop-distribution from the headers (or from the
optional Prometheus meters).

## Modules

| Module | Coordinate | Pulls | Use it for |
|---|---|---|---|
| **isotope-core** | `ai.signalroom:isotope-core` | Jackson, SLF4J (`kafka-clients` is `compileOnly`) | Trace **propagation** — interceptor, headers, consume markers. |
| **isotope-metrics** | `ai.signalroom:isotope-metrics` | isotope-core + Micrometer/Prometheus | Optional `/metrics` exporter for the stateless reports. |

`isotope-core` never depends on a metrics library: emission routes through a
no-op `IsotopeMetricsSink` until `isotope-metrics` registers the Prometheus one,
so propagation runs with zero metrics overhead.

## Install (Gradle, GitHub Packages)

```groovy
repositories {
    maven { url 'https://maven.pkg.github.com/j3-signalroom/kafka-isotope' }
}
dependencies {
    implementation 'ai.signalroom:isotope-core:0.16.0'
    implementation 'ai.signalroom:isotope-metrics:0.16.0' // optional — only for Prometheus
}
```

See **[isotope-core/README.md](isotope-core/README.md)** for the full adopter
quickstart (registering the interceptor, adopting/marking on consume, starting
the exporter).

## Build

```bash
./gradlew build                 # compile + test both modules
./gradlew publishToMavenLocal   # install 0.16.0 into ~/.m2 for local consumers
./gradlew publish               # publish to GitHub Packages (needs a token)
```

The library targets **Java 17** bytecode for wide adoption (toolchain builds on
21). Publishing to GitHub Packages reads credentials from the `gpr.user` /
`gpr.key` Gradle properties or the `GITHUB_ACTOR` / `GITHUB_TOKEN` env vars.

## Demo

A runnable reference pipeline (Confluent Platform / Confluent Cloud on Minikube,
the seven Flink reports, and a one-command Prometheus + Grafana showcase) lives
in the companion repo:
[j3-signalroom/confluent-kafka-isotope](https://github.com/j3-signalroom/confluent-kafka-isotope).
