/**
 * Copyright (c) 2026 Jeffrey Jonathan Jennings
 *
 * @author Jeffrey Jonathan Jennings (J3)
 *
 *
 */
package ai.signalroom.kafka.isotope.otel;

/**
 * What the span writer's queue holds: either a {@link SpanEvent} built on the
 * calling thread, or an {@link AckedHop} whose decoding was deferred to the
 * writer thread because it arrived on the producer's I/O thread.
 */
sealed interface QueuedSpan permits SpanEvent, AckedHop {
}
