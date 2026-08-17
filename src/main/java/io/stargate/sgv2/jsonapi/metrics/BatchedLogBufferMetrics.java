package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metrics for the billing buffer in {@link io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer}
 */
public final class BatchedLogBufferMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedLogBufferMetrics.class);

  private final AtomicBoolean bufferRegister = new AtomicBoolean(false);

  private final MeterRegistry meterRegistry;
  private final String prefix;

  private final Counter offered;
  private final Counter dropped;

  /**
   */
  public BatchedLogBufferMetrics(MeterRegistry meterRegistry, String prefix) {

    this.meterRegistry = Objects.requireNonNull(meterRegistry,  "meterRegistry must not be null");
    this.prefix = Objects.requireNonNull(prefix, "prefix must not be null");
    if (prefix.isBlank()) {
      throw new IllegalArgumentException("prefix must not be blank");
    }

    this.offered = meterRegistry.counter(prefix + ".buffer.offered");
    this.dropped = meterRegistry.counter(prefix + ".buffer.dropped" );
    // Note: not recording events dropped at shutdown as a metric because when shutting down
    // the metrics still need to be scrapped to be useful. Do it as a log message that is persistent.
  }

  public void registerBuffer(BatchedLogBuffer buffer) {

    if (!bufferRegister.compareAndSet(false, true)) {
      throw new IllegalStateException("registerBuffer() already called");
    }

    Gauge.builder(prefix + ".buffer.head_age_ms",
                    () -> buffer.headEntryAge().toMillis())
            .register(meterRegistry);
    Gauge.builder(prefix + ".buffer.size", buffer::size)
            .register(meterRegistry);
    Gauge.builder(prefix + ".buffer.remaining_capacity", buffer::remainingCapacity)
            .register(meterRegistry);
    Gauge.builder(prefix + ".buffer.queued_bytes", buffer::queuedBytes)
            .register(meterRegistry);
  }


  public void recordOffered() {
    offered.increment();
  }

  public void recordDropped() {
    dropped.increment();
  }

}
