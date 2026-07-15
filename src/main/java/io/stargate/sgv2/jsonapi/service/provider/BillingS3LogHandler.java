package io.stargate.sgv2.jsonapi.service.provider;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers {@code billing.events} log lines and ships them to S3 in sealed batches.
 *
 * <p>Pure orchestration: {@link BillingQueue} owns the batching policy (when a batch seals), the
 * {@link AsyncBatchUploader} owns what lands in the bucket (key layout, body encoding), and this
 * handler wires them together — a non-blocking publish path, seal- and age-triggered flushes under
 * bounded upload concurrency, metrics, and a deadline-bounded drain on close.
 */
public final class BillingS3LogHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(BillingS3LogHandler.class);

  // ---- Collaborators ----
  private final AsyncBatchUploader uploader;
  private final BillingMetrics metrics;
  private final BillingQueue buffer;

  // ---- Flush pipeline ----
  private final int uploadConcurrency; // max flushes (S3 PUTs) in flight at once
  private final AtomicInteger inFlight = new AtomicInteger(0);
  private final ScheduledFuture<?> ageFlushTask;

  private final Duration shutdownTimeout;

  public BillingS3LogHandler(
      BillingS3ExportConfig config, AsyncBatchUploader uploader, MeterRegistry meterRegistry) {
    this(
        uploader,
        meterRegistry,
        config.maxEvents(),
        config.maxBytes(),
        config.maxAge(),
        config.queueCapacity(),
        config.uploadConcurrency(),
        config.shutdownTimeout());
  }

  @VisibleForTesting
  BillingS3LogHandler(
      AsyncBatchUploader uploader,
      MeterRegistry meterRegistry,
      int maxEvents,
      long maxBytes,
      Duration maxAge,
      int queueCapacity,
      int uploadConcurrency,
      Duration shutdownTimeout) {
    if (uploadConcurrency < 1) {
      throw new IllegalArgumentException(
          "s3.upload-concurrency must be >= 1 (was " + uploadConcurrency + ")");
    }
    requirePositive(maxAge, "max-age");
    requirePositive(shutdownTimeout, "shutdown-timeout");
    this.uploader = uploader;
    this.uploadConcurrency = uploadConcurrency;
    this.shutdownTimeout = shutdownTimeout;
    this.buffer = new BillingQueue(maxEvents, maxBytes, queueCapacity);
    this.metrics = new BillingMetrics(meterRegistry, buffer::size, queueCapacity);
    this.ageFlushTask =
        Infrastructure.getDefaultWorkerPool()
            .scheduleAtFixedRate(
                this::onAgeTick, maxAge.toMillis(), maxAge.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static void requirePositive(Duration value, String property) {
    if (value == null || value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(
          "stargate.jsonapi.billing.s3." + property + " must be > 0 (was " + value + ")");
    }
  }

  // ============================================================
  // java.util.logging.Handler
  // ============================================================

  @Override
  public void publish(LogRecord record) {
    if (record == null) {
      return;
    }
    // Producer contract (DefaultBilling):  the message is the final JSON line, logged without {}
    // placeholders. This handler never runs a Formatter, so a parameterized call would ship its
    // raw template. getInstant() is when the producer logged it — within microseconds of the
    // "timestamp" it embedded in the JSON, and the object key only needs minute resolution.
    String line = record.getMessage();
    if (line == null || line.isBlank()) {
      return;
    }
    metrics.recordOffered();
    if (!buffer.offer(record.getInstant(), line)) {
      // Bounded buffer full: drop and count
      metrics.recordDropped();
      return;
    }
    maybeFlush();
  }

  @Override
  public void flush() {
    // No-op: shipping is seal-triggered (publish) and age-triggered (tick); close() drains.
  }

  /**
   * Drains what remains through the normal flush pipeline, bounded by {@code shutdownTimeout}. The
   * budget only bites when S3 is already failing: it converts a silent SIGKILL into a logged count
   * of abandoned events and lets the rest of shutdown proceed.
   */
  @Override
  public void close() {
    // Don't interrupt a tick already running; the in-flight wait below covers it.
    ageFlushTask.cancel(false);
    long deadlineNanos = System.nanoTime() + shutdownTimeout.toNanos();

    // Pump the pipeline until the buffer is drained: tryFlush() is a no-op while all slots are
    // busy, and every settled upload frees a slot for the next batch.
    while (!buffer.isEmpty() && System.nanoTime() < deadlineNanos) {
      tryFlush();
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    // Let in-flight uploads (ours and any started before close) settle within the budget.
    while (inFlight.get() > 0 && System.nanoTime() < deadlineNanos) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }

    int queuedAbandoned = buffer.size();
    int inFlightAbandoned = inFlight.get();
    if (queuedAbandoned > 0 || inFlightAbandoned > 0) {
      LOG.warn(
          "Billing S3 export shutdown budget ({}) exhausted: dropping {} buffered events,"
              + " abandoning {} in-flight uploads",
          shutdownTimeout,
          queuedAbandoned,
          inFlightAbandoned);
    }
    uploader.close(); // aborts anything still in flight
  }

  // ============================================================
  // Flush pipeline
  // ============================================================

  /** Seal-triggered flush: ship when the buffer has a full batch by count or bytes. */
  private void maybeFlush() {
    if (buffer.shouldFlush()) {
      tryFlush();
    }
  }

  /**
   * Age-triggered flush: every {@code maxAge} tick ships whatever is buffered, sealed or not, so no
   * event waits longer than ~{@code maxAge} even under trickle traffic.
   */
  private void onAgeTick() {
    try {
      if (!buffer.isEmpty()) {
        tryFlush();
      }
    } catch (Throwable t) {
      LOG.error("Billing S3 export age-flush tick failed", t);
    }
  }

  /**
   * Claims an in-flight slot (non-blocking CAS, at most {@link #uploadConcurrency} held) and, on
   * success, drains + uploads one batch asynchronously. When the upload settles the slot is
   * released and the seal condition re-checked: a full batch may have accumulated meanwhile.
   */
  private void tryFlush() {
    int prev = inFlight.getAndUpdate(n -> n < uploadConcurrency ? n + 1 : n);
    if (prev >= uploadConcurrency) {
      return;
    }
    Uni.createFrom()
        .item(buffer::drain)
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .flatMap(this::uploadBatch)
        .eventually(
            () -> {
              inFlight.getAndDecrement();
              maybeFlush();
            })
        .subscribe()
        .with(ignored -> {}, failure -> LOG.error("Billing S3 export flush failed", failure));
  }

  /** Uploads one batch; never fails the pipeline — a batch that exhausts retries is counted. */
  private Uni<Void> uploadBatch(BillingQueue.Batch batch) {
    if (batch.isEmpty()) {
      return Uni.createFrom().voidItem();
    }
    int size = batch.size();
    return uploader
        .upload(batch)
        .onItem()
        .invoke(() -> metrics.recordBatchDelivered(size))
        .onFailure()
        .invoke(t -> LOG.error("Failed to upload billing S3 batch ({} size)", size, t))
        .onFailure()
        .recoverWithItem(
            () -> {
              metrics.recordBatchFailed(size);
              return null;
            });
  }

  /**
   * Uploads one sealed batch to the export destination; owns the object key and body encoding.
   * Implementations must tolerate concurrent calls.
   */
  @FunctionalInterface
  public interface AsyncBatchUploader extends AutoCloseable {
    Uni<Void> upload(BillingQueue.Batch batch);

    @Override
    default void close() {}
  }
}
