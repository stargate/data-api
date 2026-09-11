package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metrics for the billing buffer in {@link
 * io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer}
 */
public final class BatchedLogBufferMetrics extends MetricsBase {

  private final AtomicBoolean bufferRegister = new AtomicBoolean(false);

  public final Counter offered;
  public final Counter dropped;

  // these are nullable, they are set when registerBuffer is called.
  public TimeGauge headAgeMs;
  public Gauge size;
  public Gauge remainingCapacity;
  public Gauge bytes;

  /** */
  public BatchedLogBufferMetrics(MeterRegistry meterRegistry, String prefix) {
    super(meterRegistry, prefix);

    this.offered = newCounter("buffer.offered");
    this.dropped = newCounter("buffer.dropped");

    // Note: not recording events dropped at shutdown as a metric because when shutting down
    // the metrics still need to be scrapped to be useful. Do it as a log message that is
    // persistent.
  }

  public void registerBuffer(BatchedLogBuffer buffer) {

    if (!bufferRegister.compareAndSet(false, true)) {
      throw new IllegalStateException("registerBuffer() already called");
    }

    headAgeMs =
        newTimeGauge(
            "buffer.head_age_ms", () -> buffer.headEntryAge().toMillis(), TimeUnit.MILLISECONDS);
    size = newGauge("buffer.size", buffer::size);
    remainingCapacity = newGauge("buffer.remaining_capacity", buffer::remainingCapacity);
    bytes = newGauge("buffer.bytes", buffer::queuedBytes);
  }

  public void incrementOffered() {
    offered.increment();
  }

  public void incrementDropped() {
    dropped.increment();
  }
}
