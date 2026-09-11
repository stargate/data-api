package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.stargate.sgv2.jsonapi.service.billing.BatchedLogBuffer;
import java.util.Objects;

/** Metrics for billing events, mostly around what is sent to S3 */
public final class BatchedLogUploaderMetrics extends MetricsBase {

  public final Counter uploadedBatches;
  public final Counter uploadedBytes;
  public final Timer uploadedHeadAgeMs;
  public final Counter uploadedEvents;

  public final Counter failedBatches;
  public final Counter failedEvents;

  /** */
  public BatchedLogUploaderMetrics(MeterRegistry meterRegistry, String prefix) {
    super(meterRegistry, prefix);

    this.uploadedBatches = newCounter("uploaded.batches");
    this.uploadedBytes = newCounter("uploaded.bytes");
    this.uploadedEvents = newCounter("uploaded.events");
    this.uploadedHeadAgeMs = newTimer("uploaded.oldest_event");

    this.failedBatches = newCounter("failed.batches");
    this.failedEvents = newCounter("failed.events");
  }

  public void recordBatchDelivered(BatchedLogBuffer.Batch batch) {

    Objects.requireNonNull(batch, "batch must not be null");
    uploadedBatches.increment();
    uploadedBytes.increment(batch.bytes());
    uploadedHeadAgeMs.record(batch.oldestEventAtDuration());
    uploadedEvents.increment(batch.size());
  }

  public void recordBatchFailed(BatchedLogBuffer.Batch batch) {

    Objects.requireNonNull(batch, "batch must not be null");
    failedBatches.increment();
    failedEvents.increment(batch.size());
  }
}
