package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Logging handler designed to be used with the Billing system. It accepts billing event log
 * messages, batches them, and then sends to S3.
 *
 * <p>See {@link BillingS3Lifecycle} for setup.
 *
 * <p>Designed to be used in two ways:
 *
 * <ul>
 *   <li>Registered as a log {@link Handler} for the logger created with {@link
 *       DefaultBilling#BILLING_LOGGER_NAME} so the {@link #publish(LogRecord)} is called by the
 *       logging system from multiple threads.
 *   <li>Called on a deamon worker thread the {@link #startUploading()} function will run until
 *       {@link #close()} is called, when running it periodiclky checks the buffer of log messages
 *       and uploads using a {@link AsyncBatchedLogUploader} to S3 (normally)
 * </ul>
 */
public final class BillingUploadingLogHandler extends Handler {

  // Logger for this handler, not the destination we are sending events to.
  private static final Logger LOGGER = LoggerFactory.getLogger(BillingUploadingLogHandler.class);

  /**
   * When true means the Handler has been closed via {@link #close()} and it will silently drop any
   * further calls to publish log entries. This also cause the upload thread to empty the buffer
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Disposable permitting system for forcing wakeup in the uploading thread. startUploading() will
   * tryAcquire() but because the permit count is 0 will always timeout, this is the timeout to wake
   * and check buffer. When we want to force wakeup, e.g. flush(), we call release() that means any
   * tryAcquire() returns and decrements count to 0. Resetting back to initial state. Because the
   * wakeup permit lasts until tryAcquire it removes race conditions that could happen when
   * flush()/notify() on an object lands before the upload thread is in wait() - if we used
   * Object.notify() and .wait()
   */
  private final Semaphore wakeupPermit = new Semaphore(0);

  /**
   * There is only 1 permit for the upload process to be running. When {@link #startUploading()}
   * starts it takes the permit, gives it back when the function exits (after {@link #close()}.
   * close() uses this to make sure uploading has finished.
   */
  private final Semaphore uploadPermit = new Semaphore(1);

  private final AsyncBatchedLogUploader uploader;
  private final BatchedLogBuffer buffer;
  private final Duration uploadSleepDuration;
  private final Duration uploaderSafetyDeadline;
  private final Duration uploadShutdownDeadline;

  /**
   * Creates a new instance, see {@link BillingS3Lifecycle}
   *
   * @param buffer A configured buffer that is used to buffer incoming log messages
   * @param uploader {@link AsyncBatchedLogUploader} to upload messages when the buffer has a new
   *     batch.
   * @param uploadSleepDuration Duration the thread running {@link #startUploading()} will sleep
   *     waiting for a new batch from the buffer.
   * @param uploaderSafetyDeadline Duration to wait for the uploader to return from uploading. NOTE:
   *     the uploader should have its own internal timeouts, this is to prevent the uploading thread
   *     blocking.
   * @param uploadShutdownDeadline Duration a call to {@link #close()} should wait for the uploading
   *     thread to complete flushing remaining events.
   */
  BillingUploadingLogHandler(
      BatchedLogBuffer buffer,
      AsyncBatchedLogUploader uploader,
      Duration uploadSleepDuration,
      Duration uploaderSafetyDeadline,
      Duration uploadShutdownDeadline) {

    this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
    this.uploader = Objects.requireNonNull(uploader, "uploader must not be null");
    this.uploadSleepDuration =
        Objects.requireNonNull(uploadSleepDuration, "uploadSleepDuration must not be null");
    this.uploaderSafetyDeadline =
        Objects.requireNonNull(uploaderSafetyDeadline, "uploaderSafetyDeadline must not be null");
    this.uploadShutdownDeadline =
        Objects.requireNonNull(uploadShutdownDeadline, "uploadShutdownDeadline must not be null");
  }

  /**
   * WARNING - sets the flag for closing but does not run the full close. just here for testing how
   * uploading wakes up when flush called.
   */
  @VisibleForTesting
  void unsafeClose() {
    LOGGER.warn("WARNING - unsafeClose() called, must only be used in testing");
    isClosed.set(true);
  }

  /**
   * WARNING - acquires the upload permit, this stops the startUpload() function and close() from
   * working normally. For testing only.
   */
  @VisibleForTesting
  void unsafeAcquireUploadPermit() {
    LOGGER.warn("WARNING - unsafeAcquireUploadPermit() called, must only be used in testing");
    uploadPermit.acquireUninterruptibly();
  }

  // ============================================================
  // Overrides for java.util.logging.Handler
  // ============================================================

  /**
   * Buffers and then publishes the record to S3.
   *
   * @param record description of the log event. A null record is silently ignored and is not
   *     published.
   */
  @Override
  public void publish(LogRecord record) {

    // Sanity check
    if (record == null) {
      return;
    }

    if (isClosed.get()) {
      LOGGER.warn("publish() - called when closed, dropping record:{}", record);
      return;
    }

    // buffer handles metrics
    if (!buffer.offer(record)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "publish() - buffer.offer() rejected, dropping record:{}", record.getMessage());
      }
    } else if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("publish() - buffer.offer() accepted, record:{}", record.getMessage());
    }
  }

  /**
   * Wakes up the uploading thread to check the buffer for batches.
   *
   * <p>This will only drain the buffer fully (i.e. including partial batches) if {@link #close()}
   * is called or {@link #isClosed} is set.
   */
  @Override
  public void flush() {
    maybeTrace("flush() - called");
    notifyUploading();
  }

  /**
   * Closes the LogHandler so that it will drop any records sent to {@link #publish(LogRecord)} and
   * drain the buffer fully to send all batches to the uploader.
   */
  @Override
  public void close() {

    LOGGER.info(
        "closing() - marking handler closed, flushing, and waiting for uploads to complete. uploadShutdownDeadline:{}",
        uploadShutdownDeadline);

    // mark as closed to stop accepting further events and tell the upload thread
    // to drain the buffer fully.
    isClosed.set(true);
    flush();

    try {
      // Check uploading is not running by trying to get the single uploading permit
      if (!uploadPermit.tryAcquire(uploadShutdownDeadline.toMillis(), TimeUnit.MILLISECONDS)) {
        LOGGER.warn(
            "close() - Failed to get uploading permit, upload failed to stop. uploadShutdownDeadline:{}",
            uploadShutdownDeadline);
      } else {
        uploadPermit.release();
        LOGGER.debug("close() - acquired uploading permit, uploading has completed.");
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

  /**
   * Call this on a worker thread to start uploading, will start a loop of waiting for batches from
   * the buffer and uploading them.
   *
   * <p>Call {@link #close()} to signal this function to stop waiting, upload all events, and
   * return.
   */
  void startUploading() {

    LOGGER.info("startUploading() - handler:{}, buffer:{}, uploader:{}", this, buffer, uploader);

    boolean hasUploadPermit = false;
    BatchedLogBuffer.Batch batch;
    try {
      if (!(hasUploadPermit = uploadPermit.tryAcquire())) {
        throw new IllegalStateException(
            "startUploading() - unable to acquire uploadPermit, was function already called?");
      }

      while (true) {

        // if the handler is closed we do not want to go to sleep again because it is closing
        // down.
        if (!isClosed.get()) {
          try {
            maybeTrace(
                "startUploading() - waiting for wakeupPermit. isClosed:{}, uploadSleepDuration:{}",
                isClosed.get(),
                uploadSleepDuration);
            var acquiredWakePermit =
                wakeupPermit.tryAcquire(uploadSleepDuration.toMillis(), TimeUnit.MILLISECONDS);
            // is not important if we got a permit to wake, or timed out, just for logging
            maybeTrace(
                "startUploading() - wakeup permit or timeout, isClosed:{}, acquiredWakePermit:{}",
                isClosed.get(),
                acquiredWakePermit);

          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            dumpBuffer();
            return;
          }
        } else {
          maybeTrace(
              "startUploading() - not waiting for wakeupPermit because isClosed:{}",
              isClosed.get());
        }

        // Get the next batches, if isClosed is true then we want to drain all events
        // which may mean creating a batch when we do not have a full one.
        while ((batch = buffer.nextBatch(isClosed.get())) != null) {
          uploadBatch(batch);
        }

        if (isClosed.get()) {
          // Handler is closing down, time to get out of this crazy loop
          break;
        }
      }
    } finally {
      // release the uploading permit if we have it, to signal close() to stop waiting
      if (hasUploadPermit) {
        uploadPermit.release();
        maybeTrace("startUploading() - releasing upload permit");
      } else {
        maybeTrace("startUploading() - upload permit was not acquired, not releasing");
      }
    }

    if (!buffer.isEmpty()) {
      LOGGER.warn(
          "startUploading() - finished with abandoned billing events, billingQueue.size():{} ",
          buffer.size());
    }

    LOGGER.info(
        "startUploading() - stopped uploading. handler:{}, buffer:{}, uploader:{}",
        this,
        buffer,
        uploader);
  }

  /** Signals the thread running {@link #startUploading()} to wake up and check for work. */
  private void notifyUploading() {
    // Adds a permit to the wakeupPermit so the uploading thread will wakeup and do some work.
    wakeupPermit.release();
  }

  private static void maybeTrace(String message, Object... args) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(message, args);
    }
  }

  /** Creates a Uni that will upload the provided batch. */
  private void uploadBatch(BatchedLogBuffer.Batch batch) {

    LOGGER.info(
        "uploadBatch() - starting to upload. uploaderSafetyDeadline:{}, batch:{}",
        uploaderSafetyDeadline,
        batch);

    // while the uploader should take of all the timeout and retry logic
    // as a client of the uploader adding a safety timeout here in case it breaks.
    // we do not want the uploader hanging to block this thread.

    // using deferred so that an error in upload() before it returns the Uni is then
    // treated as an error through the Uni pipeline
    var uploadResult =
        Uni.createFrom()
            .deferred(() -> uploader.upload(batch))
            .ifNoItem()
            .after(uploaderSafetyDeadline)
            .fail()
            .onFailure()
            .recoverWithItem(
                t -> onUploaderFailure(batch, t)) // TimeoutException id deadline exceeded
            .await()
            .indefinitely(); // the deadline above covers it, this causes deferred to start exec'ing

    if (uploadResult.throwable() == null) {
      onBatchSuccess(uploadResult);
    } else {
      onBatchFailure(uploadResult);
    }
  }

  /**
   * There was an unhandled error from the uploader().
   *
   * <p>Could be from in upload() before it returned or from running the Uni to do the upload. Just
   * map this unhandled back into the UploadResult so we can deal with error in standard way
   */
  private AsyncBatchedLogUploader.UploadResult onUploaderFailure(
      BatchedLogBuffer.Batch batch, Throwable throwable) {
    LOGGER.error(
        "onUploaderFailure() - throwable from uploader, adding to UploadResult.  batch:{}, throwable:{}",
        batch,
        throwable.toString());
    return new AsyncBatchedLogUploader.UploadResult(batch, throwable);
  }

  private void onBatchSuccess(AsyncBatchedLogUploader.UploadResult uploadResult) {
    LOGGER.info("onBatchSuccess() - successfully uploaded batch:{}", uploadResult.batch());
  }

  private void onBatchFailure(AsyncBatchedLogUploader.UploadResult uploadResult) {
    LOGGER.error("onBatchFailure() - failed to upload batch:{}", uploadResult.batch());
    dumpBuffer();
  }

  /** TODO: dump buffer or a failed batch to regular logs or whatever */
  private void dumpBuffer() {
    LOGGER.warn("dumpBuffer() - DUMP BUFFER CALLED, NOT IMPLEMENTED YET!");
  }

  private void dumpBatch(BatchedLogBuffer.Batch batch) {
    LOGGER.warn("dumpBuffer() - DUMP BATCH CALLED, NOT IMPLEMENTED YET!");
  }

  @Override
  public String toString() {
    return new StringBuilder(classSimpleName(this) + "{")
        .append("uploadSleepDuration=")
        .append(uploadSleepDuration)
        .append(", uploadShutdownDeadline=")
        .append(uploadShutdownDeadline)
        .append(", uploaderSafetyDeadline=")
        .append(uploaderSafetyDeadline)
        .append(", isClosed=")
        .append(isClosed)
        .append(", wakeupPermit.availablePermits=")
        .append(wakeupPermit.availablePermits())
        .append(", uploadPermit.availablePermits=")
        .append(uploadPermit.availablePermits())
        .append("}")
        .toString();
  }
}
