package io.stargate.sgv2.jsonapi.service.provider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * During normal operation, {@code offered = flushed + failed + dropped(capacity) + queue.depth +
 * events in in-flight batches}.
 *
 * <p>At shutdown, buffered events left after the drain budget are added to {@code
 * dropped(shutdown)}. Final counters can be lower than {@code offered} if a concurrent publish
 * misses the final queue snapshot or an in-flight upload does not settle before process exit.
 *
 * <p>{@code last_delivery.epoch_seconds} is the delivery heartbeat.
 */
public final class BillingMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(BillingMetrics.class);
  private static final long DROP_WARN_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(10);

  private final Counter offered;
  private final Counter droppedCapacity;
  private final Counter droppedShutdown;
  private final Counter flushed;
  private final Counter failed;
  private final Counter batchesUploaded;
  private final Counter batchesFailed;
  private final AtomicLong lastDeliveryEpochSeconds = new AtomicLong(0);
  private final AtomicLong lastDropWarnNanos;
  private final long queueCapacity;

  /**
   * @param depthSource live queue depth, exposed read-only as {@code billing.s3.queue.depth}
   * @param queueCapacity quoted in the buffer-full warning
   */
  public BillingMetrics(
      MeterRegistry meterRegistry, Supplier<Number> depthSource, long queueCapacity) {
    this.queueCapacity = queueCapacity;
    this.lastDropWarnNanos = new AtomicLong(System.nanoTime() - DROP_WARN_INTERVAL_NANOS);
    this.offered = meterRegistry.counter("billing.s3.events.offered");
    this.droppedCapacity = meterRegistry.counter("billing.s3.events.dropped", "reason", "capacity");
    this.droppedShutdown = meterRegistry.counter("billing.s3.events.dropped", "reason", "shutdown");
    this.flushed = meterRegistry.counter("billing.s3.events.flushed");
    this.failed = meterRegistry.counter("billing.s3.events.failed");
    this.batchesUploaded = meterRegistry.counter("billing.s3.batches.uploaded");
    this.batchesFailed = meterRegistry.counter("billing.s3.batches.failed");
    // Catches stalls with no failures to count (e.g. the flush trigger died): alert on staleness
    // gated by offered/depth, so idle time isn't mistaken for a dead export.
    Gauge.builder(
            "billing.s3.last_delivery.epoch_seconds",
            lastDeliveryEpochSeconds,
            AtomicLong::doubleValue)
        .description("Epoch seconds of the last successful batch delivery")
        .register(meterRegistry);
    Gauge.builder("billing.s3.queue.depth", depthSource)
        .description("Billing events buffered in memory, not yet drained")
        .register(meterRegistry);
  }

  /** A line was handed to the handler (counted before the capacity check). */
  public void recordOffered() {
    offered.increment();
  }

  /** A line was dropped on a full buffer; warns rate-limited so a sustained stall stays visible. */
  public void recordDropped() {
    droppedCapacity.increment();
    long now = System.nanoTime();
    long prev = lastDropWarnNanos.get();
    // Rate limit for the buffer-full warning: keeps a drop storm from spamming the logs while
    // still surfacing a second stall long after the first.
    if (now - prev >= DROP_WARN_INTERVAL_NANOS && lastDropWarnNanos.compareAndSet(prev, now)) {
      LOG.warn(
          "Billing S3 export backlog full ({} events): shedding billing events because S3 uploads"
              + " are slower than ingest. Every shed line is counted by billing.s3.events.dropped.",
          queueCapacity);
    }
  }

  /** Events still buffered when the shutdown budget ran out; close() logs the tombstone. */
  public void recordAbandonedAtShutdown(int size) {
    droppedShutdown.increment(size);
  }

  /** A batch of event lines landed in S3; bumps the delivery heartbeat. */
  public void recordBatchDelivered(int size) {
    flushed.increment(size);
    batchesUploaded.increment();
    lastDeliveryEpochSeconds.set(Instant.now().getEpochSecond());
  }

  /** A batch of event lines was given up after the uploader exhausted its retries. */
  public void recordBatchFailed(int size) {
    failed.increment(size);
    batchesFailed.increment();
  }
}
