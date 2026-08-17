package io.stargate.sgv2.jsonapi.service.billing;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Logging handler designed to be used wioth the Billing system. It accpets billing event log
 * messges, batches them, and then sends to S3.
 *
 * <p>See {@link BillingS3HandlerInstaller} for setup. // AI SLOP BELOW JUL handler that turns
 * {@code billing.events} log lines into batched S3 objects.
 *
 * <p>Division of labor: {@link BatchedLogBuffer} decides when a batch seals, {@link AsyncBatchedLogUploader}
 * decides what an S3 object looks like, and this class decides when uploads run — the flush
 * triggers (seal on publish, age tick, drain on close), the upload-concurrency gate, and metrics.
 *
 * <p>Delivery is at-most-once by design: publish never waits for queue capacity, full buffers drop
 * new lines, and close drains best-effort within {@code shutdownTimeout}.
 */
public final class BillingS3LogHandler extends Handler {

  // Logger for this handler, not the destination we are sending events to.
  private static final Logger LOGGER = LoggerFactory.getLogger(BillingS3LogHandler.class);

  /**
   * Duration the upload thread sleeps between uploading. After all available batches have been
   * uploaded, sleeps for this long waiting for the {@link #wakeupSignal}
   */
  private static final long UPLOAD_SLEEP_MS = 1000;

  /**
   * When signaled this object wakes up the uploading thread to immediately get to work. Used as
   * part of the close mechanism to trigger uploading to complete. We do not signal the upload
   * everytime a producer calls {@link #publish(LogRecord)}.
   */
  private final Object wakeupSignal = new Object();

  /**
   * When true means the Handler has been closed via {@link #close()} and it will silently drop any
   * further calls to publish log entries. This also cauese the upload thread to empty the queue
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Started at 1 and then decremented in {@link #startUploading()} when it exists so we know we
   * have finished uploading.
   */
  private final CountDownLatch uploadingFinished = new CountDownLatch(0);

  private final AsyncBatchedLogUploader uploader;
  private final BatchedLogBuffer batchedLogBuffer;

  @VisibleForTesting
  BillingS3LogHandler(
          AsyncBatchedLogUploader uploader, BatchedLogBuffer batchedLogBuffer) {

    this.batchedLogBuffer = batchedLogBuffer;
    this.uploader = uploader;
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

    // Sanity check
    if (record == null) {
      return;
    }

    if (isClosed.get()) {
      LOGGER.warn("publish() - called when closed, dropping record:{}", record);
    }

    // buffer handles metrics
    if (!batchedLogBuffer.offer(record)) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("publish() - dropped record:{}", record);
      }
    }
    // flushing runs every second so nothing more to do
  }

  @Override
  public void flush() {
    // wakeup the upload thread to send eveything it can.
    // NOTE: this will only drain the buffer if isClosed() is true
    notifyUploading();
  }

  /**
   * Drains what remains through the normal flush pipeline, bounded by {@code shutdownTimeout}. The
   * budget only bites when S3 is already failing: it converts a silent SIGKILL into a logged count
   * of abandoned events and lets the rest of shutdown proceed.
   */
  @Override
  public void close() {

    // mark as closed to stop accepting further events and tell the upload thread
    // to drain the bugger/
    isClosed.set(true);
    flush();

    try {
      // waiting for the uploading thead to signal it has sent all events in the buffer
      if (!uploadingFinished.await(30, TimeUnit.SECONDS)) {
        LOGGER.warn("close() - Billing upload loop did not stop within 30s, interrupting");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("close() - Interrupted waiting for billing upload loop to finish");
    } finally {
      uploader.close();
    }
  }

  // ============================================================
  // Flush pipeline
  // ============================================================

  /** Called on a worker thread to start uploading log records. */
  void startUploading() {

    BatchedLogBuffer.Batch batch;
    try {
      while (true) {

        synchronized (wakeupSignal) {
          // if the handler is closed we do not want to go to sleep again because it is closing
          // down.
          if (!isClosed.get()) {
            try {
              // waiting will release the synchronized monitor
              wakeupSignal.wait(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }

        // Get the next batches, if isClosed is true then we want to drain all events
        // which may mean creating a batch when we do not have a full one.

        while ((batch = batchedLogBuffer.nextBatch(isClosed.get())) != null) {

          // calling await() on the Uni from deferBatch causes the uni to start running
          // indefinitely() is bounded by the 10s ifNoItem() timeout inside deferBatch(),
          // and failures are recovered there, so this neither hangs nor throws.
          deferBatch(batch).await().indefinitely();
        }

        if (isClosed.get()) {
          // Handler is closing down, time to get out of this crazy loop
          break;
        }
      }
    } finally {
      // record if there are any abandoned events
      if (!batchedLogBuffer.isEmpty()) {
        LOGGER.warn(
            "start() - finished with abandoned billing events, billingQueue.size():{} ",
            batchedLogBuffer.size());
      }

      // reset the latch so the call at close() can exit.
      uploadingFinished.countDown();
    }
  }

  /**
   * Signals to the uploading thread that it should wakeup and do some work.
   *
   * <p>The uploading thread runs repeated checks for new batches, this is only needed to wakeup as
   * part of closing
   */
  private void notifyUploading() {
    synchronized (wakeupSignal) {
      wakeupSignal.notifyAll();
    }
  }

  /**
   * Creates a Uni that will upload the provided batch.
   *
   * <p>As a deferred Uni it does not do any work until something pulls the item, so the caller (see
   * startUploading()) starts the work and can decide to wait etc.
   *
   * @param batch
   * @return
   */
  private Uni<AsyncBatchedLogUploader.UploadResult> deferBatch(BatchedLogBuffer.Batch batch) {

    // upload() is called at subscription, not when this method returns.
    // deferred also converts a synchronous throw from upload() into a Uni failure.

    return Uni.createFrom()
        .deferred(() -> uploader.upload(batch))
        .ifNoItem()
        .after(Duration.ofSeconds(10))
        .fail();
  }
}
