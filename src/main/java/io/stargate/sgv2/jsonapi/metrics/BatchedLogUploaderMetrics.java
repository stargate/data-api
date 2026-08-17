package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics for billing events, mostly around what is sent to S3
 */
public final class BatchedLogUploaderMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedLogUploaderMetrics.class);

  private final MeterRegistry meterRegistry;
  private final String prefix;

  private final Counter batchesUploaded;
  private final Counter eventsUploaded;

  private final Counter batchesFailed;
  private final Counter eventsFailed;

  /**
   */
  public BatchedLogUploaderMetrics(MeterRegistry meterRegistry, String prefix) {

    this.meterRegistry = Objects.requireNonNull(meterRegistry,  "meterRegistry must not be null");
    this.prefix = Objects.requireNonNull(prefix, "prefix must not be null");
    if (prefix.isBlank()) {
      throw new IllegalArgumentException("prefix must not be blank");
    }

    this.batchesUploaded = meterRegistry.counter(prefix + ".s3.batches.uploaded.size");
    this.batchesUploaded = meterRegistry.counter(prefix + ".s3.batches.uploaded.bytes");
    this.eventsUploaded = meterRegistry.counter(prefix + ".s3.events.uploaded.size");

    this.batchesFailed = meterRegistry.counter(prefix + ".s3.batches.failed");
    this.eventsFailed = meterRegistry.counter(prefix + ".s3.events.failed");
  }

  public void recordBatchDelivered(BatchedLogBuffer.Batch batch) {
    eventsUploaded.increment(size);
    batchesUploaded.increment();
  }

  public void recordBatchFailed(BatchedLogBuffer.Batch) {
    eventsFailed.increment(size);
    batchesFailed.increment();
  }
}
