package io.stargate.sgv2.jsonapi.service.billing;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import io.stargate.sgv2.jsonapi.metrics.BillingMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Logging handler designed to be used wioth the Billing system. It accpets billing event log
 * messges, batches them, and then sends to S3.
 * <p>
 *  See {@link BillingS3HandlerInstaller} for setup.
 * </p>
 *
 * // AI SLOP BELOW
 * JUL handler that turns {@code billing.events} log lines into batched S3 objects.
 *
 * <p>Division of labor: {@link BillingQueue} decides when a batch seals, {@link AsyncBatchUploader}
 * decides what an S3 object looks like, and this class decides when uploads run — the flush
 * triggers (seal on publish, age tick, drain on close), the upload-concurrency gate, and metrics.
 *
 * <p>Delivery is at-most-once by design: publish never waits for queue capacity, full buffers drop
 * new lines, and close drains best-effort within {@code shutdownTimeout}.
 */
public final class BillingS3LogHandler extends Handler {

  private static final Logger LOGGER = LoggerFactory.getLogger(BillingS3LogHandler.class);

  private final Object wakeupSignal = new Object();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  private final AtomicBoolean isDraining = new AtomicBoolean(false);
  private final CountDownLatch finishedLatch = new CountDownLatch(1);

  private final AsyncBatchUploader uploader;
  private final BillingMetrics billingMetrics;
  private final BillingQueue billingQueue;

  @VisibleForTesting
  BillingS3LogHandler(
      AsyncBatchUploader uploader,
      BillingQueue billingQueue,
      BillingMetrics billingMetrics) {

    this.billingQueue = billingQueue;
    this.uploader = uploader;
    this.billingMetrics = Objects.requireNonNull(billingMetrics);
  }

  private static Duration requirePositive(Duration value, String property) {
    if (value == null || value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(
          "stargate.jsonapi.billing.s3." + property + " must be > 0 (was " + value + ")");
    }
    return value;
  }

  // ============================================================
  // Overrides for java.util.logging.Handler
  // ============================================================

  @Override
  public void publish(LogRecord record) {

    if (record == null || isClosed.get()) {
      return;
    }

    // TODO: XXX : WHAT DOES MEAN "This handler never runs a Formatter, so a parameterized call would ship its"

    // Producer contract (DefaultBilling):  the message is the final JSON line, logged without {}
    // placeholders. This handler never runs a Formatter, so a parameterized call would ship its
    // raw template. getInstant() is when the producer logged it — within microseconds of the
    // "timestamp" it embedded in the JSON, and the object key only needs minute resolution.

    var line = record.getMessage();
    if (line == null || line.isBlank()) {
      return;
    }

    billingMetrics.recordOffered();
    if (!billingQueue.offer(record.getInstant(), line)) {
      // Bounded buffer full: drop and count
      billingMetrics.recordDropped();
    }
    // flushing runs every second so nothing more to do
  }

  @Override
  public void flush() {
    setWakeupSignal();
  }

  /**
   * Drains what remains through the normal flush pipeline, bounded by {@code shutdownTimeout}. The
   * budget only bites when S3 is already failing: it converts a silent SIGKILL into a logged count
   * of abandoned events and lets the rest of shutdown proceed.
   */
  @Override
  public void close() {

    isClosed.set(true);
    isDraining.set(true);
    flush();

    try {
        if (!finishedLatch.await(30, TimeUnit.SECONDS)) {
          LOGGER.warn("close() - Billing upload loop did not stop within 30s, interrupting");
        }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("close() - Interrupted waiting for billing upload loop to finish");
    }
    finally {
      uploader.close();
    }

  }

  // ============================================================
  // Flush pipeline
  // ============================================================

  private void setWakeupSignal() {
    synchronized (wakeupSignal) {
      wakeupSignal.notifyAll();
    }
  }

  void startPublishing() {

    BillingQueue.Batch batch;
    try {
      while (true) {

        synchronized (wakeupSignal) {
          if (!isDraining.get()) {
            try {
              wakeupSignal.wait(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }

        while ((batch = billingQueue.maybeDrain()) != null) {
          // indefinitely() is bounded by the 10s ifNoItem() timeout inside deferBatch(),
          // and failures are recovered there, so this neither hangs nor throws.
          deferBatch(batch).await().indefinitely();
        }

        if (isDraining.get()){
          return;
        }
      }
    }
    finally {
      billingMetrics.recordAbandonedAtShutdown(billingQueue.size());
      if (!billingQueue.isEmpty()){
        LOGGER.warn("start() - finished with abandoned billing events, billingQueue.size():{} " , billingQueue.size());
      }
      finishedLatch.countDown();
    }
  }

  private Uni<Void> deferBatch(BillingQueue.Batch batch) {

    // upload() is called at subscription, not when this method returns.
    // deferred also converts a synchronous throw from upload() into a Uni failure.

    return Uni.createFrom()
            .deferred(() -> uploader.upload(batch))
            .ifNoItem()
            .after(Duration.ofSeconds(10))
            .fail()
            .onItemOrFailure()
            .invoke(
                    (item, failure) -> {
                      if (failure != null) {
                        billingMetrics.recordBatchFailed(batch.size());
                        LOGGER.error("Failed to upload billing S3 batch ({} events)", batch.size(), failure);
                      } else {
                        billingMetrics.recordBatchDelivered(batch.size());
                      }
                    })
            .onFailure()
            .recoverWithNull();
  }

//  /** Seal-triggered flush: ship when the buffer has a full batch by count or bytes. */
//  private void maybeFlush() {
//    if (eventQueue.shouldFlush()) {
//      tryFlush();
//    }
//  }

//  /**
//   * Age trigger: every {@code maxAge} tick ships whatever is buffered, sealed or not. Deliberately
//   * no head-age check: flushing only entries older than {@code maxAge} would let an event that just
//   * missed a tick wait ~2x{@code maxAge}, while shipping unconditionally bounds every wait by one
//   * period — at the cost of an occasional small object when a tick lands just after a seal flush.
//   *
//   * <p>Catches everything: an escaped throwable would silently cancel all future runs of a
//   * fixed-rate task.
//   */
//  @VisibleForTesting
//  void onAgeTick() {
//    try {
//      if (!eventQueue.isEmpty()) {
//        tryFlush();
//      }
//    } catch (Throwable t) {
//      LOG.error("Billing S3 export age-flush tick failed", t);
//    }
//  }
//
//  /**
//   * Claims an in-flight slot (non-blocking CAS, at most {@link #uploadConcurrency} held) and, on
//   * success, drains + uploads one batch asynchronously. When the upload settles the slot is
//   * released and the seal condition re-checked: a full batch may have accumulated meanwhile.
//   */
//  private void tryFlush() {
//
//    int prev = inFlight.getAndUpdate(n -> n < uploadConcurrency ? n + 1 : n);
//    if (prev >= uploadConcurrency) {
//      return;
//    }
//    Uni.createFrom()
//        .item(eventQueue::maybeDrain)
//        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
//        .flatMap(this::uploadBatch)
//        .eventually(
//            () -> {
//              inFlight.getAndDecrement();
//              maybeFlush();
//            })
//        .subscribe()
//        .with(ignored -> {}, failure -> LOG.error("Billing S3 export flush failed", failure));
//  }

  /** Uploads one batch; never fails the pipeline — a batch that exhausts retries is counted. */
//  private Uni<Void> uploadBatch(BillingQueue.Batch batch) {
//    if (batch.isEmpty()) {
//      return Uni.createFrom().voidItem();
//    }
//    int size = batch.size();
//    // Runs immediately on subscription; deferred only turns a throw before upload() returns a Uni
//    // into a Uni failure handled below.
//    return Uni.createFrom()
//        .deferred(() -> uploader.upload(batch))
//        .onItem()
//        .invoke(() -> billingMetrics.recordBatchDelivered(size))
//        .onFailure()
//        .invoke(t -> LOG.error("Failed to upload billing S3 batch ({} events)", size, t))
//        .onFailure()
//        .recoverWithItem(
//            () -> {
//              billingMetrics.recordBatchFailed(size);
//              return null;
//            });
//  }

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
