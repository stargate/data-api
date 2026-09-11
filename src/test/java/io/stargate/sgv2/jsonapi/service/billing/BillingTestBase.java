package io.stargate.sgv2.jsonapi.service.billing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import io.stargate.sgv2.jsonapi.util.MetricsUnitAssertions;
import io.stargate.sgv2.jsonapi.util.MockClock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common code for tests around billing events being uploaded */
public abstract class BillingTestBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(BillingTestBase.class);

  // ======================================================================
  // BUFFER config and values for the tests to use
  // ======================================================================

  // want the line bytes when lines go into the buffer to be 25
  // template below is 21 bytes
  // 3 chars for the index get added in createFixture()
  // 1 char added in the buffer calc's for the `\n` to write out
  protected static final int MESSAGE_LENGTH_IN_BUFFER = 25;
  protected static final String TEMPLATE_25_CHARS = "Total of 25 chars    ";

  protected static final int MAX_BATCH_SIZE = 100;
  // The number of messages we can fit inside the max bytes setting
  protected static final int MAX_BATCH_BYTES_NUM_MESSAGES = 20;
  protected static final int MAX_BATCH_BYTES =
      MESSAGE_LENGTH_IN_BUFFER * MAX_BATCH_BYTES_NUM_MESSAGES;

  // How many full batches, tracked by max size, we want to fit in the buffer
  protected static final int BATCHES_BY_SIZE_PER_CAPACITY = 3;
  protected static final int BUFFER_CAPACITY = MAX_BATCH_SIZE * BATCHES_BY_SIZE_PER_CAPACITY;

  // number of log records we create for each feature / test
  protected static final int NUM_RECORDS = BUFFER_CAPACITY * 3;
  // when using mock clock, we set the instant for each log record to be 1 "second"
  // after the last, so we will create log records with up to
  // NUM_RECORDS of seconds past when the clock was started
  // used when testing the max age features
  protected static final Duration MAX_AGE = Duration.ofSeconds(NUM_RECORDS);
  protected static final Level LOG_LEVEL = Level.INFO;

  // ======================================================================
  // LOG HANDLER config and values for the tests to use
  // ======================================================================

  // sleep between checking the buffer, long we will normally use flush() to wake
  // for tests
  protected static final Duration UPLOAD_SLEEP_DURATION = Duration.ofSeconds(60);
  protected static final Duration UPLOAD_SLEEP_DURATION_SHORT = Duration.ofMillis(100);

  // upload must complete in this time
  protected static final Duration UPLOADER_SAFETY_DEADLINE = Duration.ofSeconds(30);
  // close() will wait this long for startUploading() to finish
  protected static final Duration UPLOAD_SHUTDOWN_DEADLINE = Duration.ofSeconds(30);
  protected static final Duration UPLOAD_SHUTDOWN_DEADLINE_SHORT = Duration.ofSeconds(1);

  protected static void threadSleep(long millis) {
    LockSupport.parkNanos(Duration.ofMillis(millis).toNanos());
  }

  protected static void waitOnLatch(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("interrupted waiting on latch", e);
    }
  }

  // *********************************************************
  // Fixture - used to group the test config and test data we need
  // *********************************************************

  /** Default fixture for buffer tests with config from the top of class */
  Fixture defaultBufferFixture() {
    return defaultBufferFixture(false);
  }

  Fixture defaultBufferFixture(boolean mockBufferClock) {
    return Fixture.createFixture(
        MAX_BATCH_SIZE,
        MAX_BATCH_BYTES,
        MAX_AGE,
        BUFFER_CAPACITY,
        NUM_RECORDS,
        LOG_LEVEL,
        TEMPLATE_25_CHARS,
        mockBufferClock,
        false,
        false,
        false);
  }

  Fixture defaultLogHandlerFixture() {
    return defaultLogHandlerFixture(true, false, false);
  }

  Fixture defaultLogHandlerFixture(
      boolean mockBuffer, boolean shortUploadSleep, boolean shortShutdownDuration) {
    return Fixture.createFixture(
        MAX_BATCH_SIZE,
        MAX_BATCH_BYTES,
        MAX_AGE,
        BUFFER_CAPACITY,
        NUM_RECORDS,
        LOG_LEVEL,
        TEMPLATE_25_CHARS,
        false,
        mockBuffer,
        shortUploadSleep,
        shortShutdownDuration);
  }

  /**
   * Tracks the config of the buffer, the buffer, the data we can use for each test to add to
   * buffer, etc.
   *
   * <p>See {@link #defaultBufferFixture(boolean)}
   */
  record Fixture(
      int maxBatchSize,
      long maxBytes,
      Duration maxAge,
      int queueCapacity,
      List<LogRecord> logRecords,
      MetricsUnitAssertions metricsUtil,
      BatchedLogBuffer buffer,
      BatchedLogBufferMetrics bufferMetrics,
      BillingUploadingLogHandler logHandler,
      AsyncBatchedLogUploader uploader,
      MockClock clock) {

    /** Create fixture, creates LogRecords that can be used to add to the buffer */
    static Fixture createFixture(
        int maxBatchSize,
        long maxBytes,
        Duration maxAge,
        int queueCapacity,
        int numLogRecords,
        Level logLevel,
        String logRecordTemplate,
        boolean mockBufferClock,
        boolean mockBuffer,
        boolean shortUploadSleepDuration,
        boolean shortUploadShutdownDeadline) {

      if (mockBuffer && mockBufferClock) {
        throw new IllegalArgumentException("cannot mock the buffer and the buffer clock");
      }
      // Make sure to initialize the mock clock before creating the log messages
      // so they are always after the start of the clock.
      var mockClock = mockBufferClock ? new MockClock() : null;

      //
      // fork the clock, we are going to use clockForRecords when creating the records
      // and will advance it 1 second for each record, the original mockClock is for
      // the buffer to use, so we let the test advance that
      // NOTE: THIS MAY RESULT IN SOME NEGATIVE AGE, BATCH uses Duration.abs() to make sure it's ok
      var clockForRecords = mockClock == null ? null : new MockClock(mockClock);

      var logRecords =
          IntStream.range(0, numLogRecords)
              .mapToObj(i -> logRecordTemplate + String.format("%03d", i))
              .map(
                  s -> {
                    var record = new LogRecord(logLevel, s);
                    if (clockForRecords != null) {
                      record.setInstant(clockForRecords.instant());
                      clockForRecords.nextSecond();
                    }
                    return record;
                  })
              .toList();

      var metricsUtil = new MetricsUnitAssertions();
      var bufferMetrics =
          new BatchedLogBufferMetrics(
              metricsUtil.registry(), BillingS3HandlerInstaller.METRICS_PREFIX);

      BatchedLogBuffer buffer;
      if (mockBuffer) {
        buffer = mock(BatchedLogBuffer.class);
        // setup for an empty buffer when calling
        when(buffer.offer(any())).thenReturn(true);
        when(buffer.isEmpty()).thenReturn(true);
        when(buffer.size()).thenReturn(0);
        when(buffer.nextBatch(anyBoolean())).thenReturn(null);
      } else {
        buffer =
            new BatchedLogBuffer(
                maxBatchSize,
                maxBytes,
                maxAge,
                queueCapacity,
                bufferMetrics,
                mockBufferClock ? mockClock : BatchedLogBuffer.DEFAULT_CLOCK);
      }
      var uploader = mock(AsyncBatchedLogUploader.class);
      var logHandler =
          new BillingUploadingLogHandler(
              buffer,
              uploader,
              shortUploadSleepDuration ? UPLOAD_SLEEP_DURATION_SHORT : UPLOAD_SLEEP_DURATION,
              UPLOADER_SAFETY_DEADLINE,
              shortUploadShutdownDeadline
                  ? UPLOAD_SHUTDOWN_DEADLINE_SHORT
                  : UPLOAD_SHUTDOWN_DEADLINE);

      return new Fixture(
          maxBatchSize,
          maxBytes,
          maxAge,
          queueCapacity,
          logRecords,
          metricsUtil,
          buffer,
          bufferMetrics,
          logHandler,
          uploader,
          mockClock);
    }

    /** Assert the buffer is full, and so offer() fails */
    void assertBufferFull(String desc, int index) {

      // although the next log record is wafer-thin, it is too much for Mr Creosote
      assertThat(buffer().offer(logRecords.get(index))).as(desc + " - fail at capacity").isFalse();

      // Running again to confirm it is still full
      assertThat(buffer().offer(logRecords.get(index)))
          .as(desc + " - second -  fail at capacity")
          .isFalse();
    }

    /**
     * Offer the log records selected by slice to the buffer, all should work, assert the buffer has
     * the items the slice selected
     */
    BufferSnapshot assertOffer(String desc, BatchedLogBufferTest.Slice slice) {

      var snapshot = BufferSnapshot.create(this);

      for (var record : slice.stream(logRecords).toList()) {
        assertThat(buffer.offer(record)).as(desc + " - assertOffer() - offering").isTrue();
      }

      snapshot.assertAll(desc, slice, true);
      return snapshot;
    }

    /**
     * Get a batch from the buffer, assert we got a batch that is legal, and assert the buffer has
     * changed by the amount of the batch
     */
    BatchedLogBuffer.Batch assertNextBatch(String desc, boolean drainFully) {

      var snapshot = BufferSnapshot.create(this);
      var batch = buffer.nextBatch(drainFully);

      // assert the batch is what we expected.
      assertThat(batch).as(desc + " - assertNextBatch() - batch is not null").isNotNull();

      assertThat(batch.size())
          .as(desc + " - assertNextBatch() - batch size <= MAX_BATCH_SIZE")
          .isLessThanOrEqualTo(maxBatchSize);
      // note: it is legal to have a batch bigger than the maxBytes, specialised tests for that
      // shoudl only happen when there is a single log record bigger than maxBytes
      assertThat(batch.bytes())
          .as(desc + " - assertNextBatch() - batch bytes <= MAX_BATCH_BYTES")
          .isLessThanOrEqualTo(maxBytes);

      // assert the buffer updated bookkeeping as we expect
      snapshot.assertAll(desc, batch);
      return batch;
    }

    // just redeclare with no exception to make it easier
    interface NoExceptionCloseable extends AutoCloseable {
      @Override
      void close();
    }

    /**
     * Start the logHandler uploading on a daemon thread, and returns a closeable for killing the
     * thread.
     *
     * @return
     */
    NoExceptionCloseable startHandlerUploading(String desc) {

      var executor =
          Executors.newSingleThreadExecutor(Thread.ofPlatform().daemon().name(desc).factory());

      var uploadingFuture =
          executor.submit(
              () -> {
                LOGGER.info("startHandlerUploading() - starting logHandler. desc:{}", desc);
                logHandler.startUploading();
              });

      return () -> {
        LOGGER.info("startHandlerUploading() - stopping logHandler. desc:{}", desc);
        try {
          // this is waiting for the uploading thread to return
          uploadingFuture.get(10, TimeUnit.SECONDS);
          executor.awaitTermination(1000, TimeUnit.MILLISECONDS);

          LOGGER.info("startHandlerUploading() - stopped logHandler. desc:{}", desc);

        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.error("startHandlerUploading() - error 1", e);
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          LOGGER.error("startHandlerUploading() - error 2", e);

          throw e.getCause() instanceof RuntimeException re
              ? re
              : new RuntimeException(e.getCause());
        } catch (TimeoutException e) {
          LOGGER.error("startHandlerUploading() - error 3", e);

          throw new RuntimeException(e);
        } finally {
          executor.shutdownNow();
        }
      };
    }
  }

  /**
   * A slice of a list, `from` is inclusive, `to` is exclusive
   *
   * <p>...
   */
  record Slice(int from, int to) {

    public <T> Stream<T> stream(List<T> list) {
      return list.stream().skip(from).limit(to - from);
    }

    public int size() {
      return to - from;
    }

    public static Slice to(int to) {
      return new Slice(0, to);
    }

    public static Slice from(int from) {
      return new Slice(from, Integer.MAX_VALUE);
    }

    public static Slice slice(int from, int to) {
      return new Slice(from, to);
    }
  }

  /**
   * Snapshot of the metadata (size etc) for the buffer, that can be used to compare how the buffer
   * metadata has changed
   *
   * <p>...
   */
  record BufferSnapshot(
      boolean isEmpty,
      int size,
      long queuedBytes,
      int remainingCapacity,
      Fixture fixture,
      MetricsUnitAssertions.MetricSnapshot metricSnapshot) {

    static BufferSnapshot create(Fixture fixture) {

      return new BufferSnapshot(
          fixture.buffer().isEmpty(),
          fixture.buffer().size(),
          fixture.buffer().queuedBytes(),
          fixture.buffer().remainingCapacity(),
          fixture,
          fixture.metricsUtil.createSnapshot());
    }

    /**
     * Assert that the current metadata values for the buffer are the values in the snapshot PLUS
     * the log records that were added by the Slice.
     */
    void assertAll(String desc, Slice slice, boolean inOrder) {
      assertBufferMetadata(desc, slice);
      assertBufferItems(desc, slice, inOrder);
    }

    /**
     * Assert that the current metadata values for the buffer are the values in the snapshot MINUS
     * the buffer entries that were removed in the batch
     */
    void assertAll(String desc, BatchedLogBuffer.Batch batch) {
      assertBufferMetadata(desc, batch);
      assertBufferItems(desc, batch);
    }

    /** current buffer metadata = snapshot + slice */
    void assertBufferMetadata(String desc, Slice slice) {

      if (slice.size() == 0) {
        assertThat(fixture.buffer().isEmpty())
            .as(desc + " - isEmpty no change after empty slice")
            .isEqualTo(isEmpty());
      } else {
        assertThat(fixture.buffer().isEmpty())
            .as(desc + " - isEmpty false after non empty slice")
            .isEqualTo(false);
      }

      assertThat(fixture.buffer().size())
          .as(desc + " - post buffer size increased by slice")
          .isEqualTo(size() + slice.size());
      // metric shoudl be the size of the buffer, does not matter what metric snapshot was
      fixture.metricsUtil.assertMetric(fixture.bufferMetrics.size, size() + slice.size());

      // Buffer offered increases, dropped does not change
      fixture.metricsUtil.assertMetric(metricSnapshot, fixture.bufferMetrics.offered, slice.size());
      fixture.metricsUtil.assertMetric(metricSnapshot, fixture.bufferMetrics.dropped, 0);

      // remaining is just a calculation, capacity - size
      fixture.metricsUtil.assertMetric(
          fixture.bufferMetrics.remainingCapacity, fixture.queueCapacity - fixture.buffer().size());

      // head age is the age of the first item in the buffer, NOTE clock overrides can change time
      var firstRecord = fixture.buffer().peekBuffer().getFirst();
      if (firstRecord != null) {
        var clockForMetric =
            fixture.clock() == null ? BatchedLogBuffer.DEFAULT_CLOCK : fixture.clock();
        // metric is calc's when read, so it may be younger (less ms) but never older (more ms)
        fixture.metricsUtil.assertMetric(
            fixture.bufferMetrics.headAgeMs,
            (a) -> {
              a.isLessThanOrEqualTo(
                  Duration.between(firstRecord.eventAt(), clockForMetric.instant())
                      .abs()
                      .toMillis());
            });
      }

      long addedBytes = 0;
      for (var record : slice.stream(fixture.logRecords).toList()) {
        addedBytes += BatchedLogBuffer.Entry.lineBytes(record.getMessage());
      }

      assertThat(fixture.buffer().queuedBytes())
          .as(desc + " - post buffer bytes increased by slice")
          .isEqualTo(queuedBytes + addedBytes);
      fixture.metricsUtil.assertMetric(fixture.bufferMetrics.bytes, queuedBytes + addedBytes);
    }

    /** current buffer metadata = snapshot - batch */
    void assertBufferMetadata(String desc, BatchedLogBuffer.Batch batch) {

      assertThat(fixture.buffer().size())
          .as(desc + " - buffer size decreased by batch size")
          .isEqualTo(size() - batch.size());

      assertThat(fixture.buffer().queuedBytes())
          .as(desc + " - buffer bytes size decreased by batch bytes")
          .isEqualTo(queuedBytes - batch.bytes());
    }

    /**
     * current buffer items contain items from slice inOrder - if we expect items in buffer to match
     * order of the fixture
     */
    void assertBufferItems(String desc, Slice slice, boolean inOrder) {

      var bufferItems = fixture.buffer().peekBuffer();

      int i = slice.from() > bufferItems.size() ? 0 : slice.from();
      for (var record : slice.stream(fixture.logRecords).toList()) {

        if (inOrder) {
          assertThat(record.getMessage())
              .as(desc + " - buffer items at position match exactly pos: " + i)
              .isEqualTo(bufferItems.get(i++).line());
        } else {

          var entry = new BatchedLogBuffer.Entry(record.getInstant(), record.getMessage());
          assertThat(bufferItems)
              .as(desc + " - buffer items contains entry: " + entry)
              .contains(entry);
        }
      }
    }

    /** current buffer items contain NONE of items in batch */
    void assertBufferItems(String desc, BatchedLogBuffer.Batch batch) {

      var peekedBuffer = fixture.buffer().peekBuffer();

      for (var batchString : batch.lines()) {

        var found = peekedBuffer.stream().anyMatch(entry -> entry.line().equals(batchString));
        assertThat(found)
            .as(desc + " - line from batch no longer in buffer: " + batchString)
            .isFalse();
      }
    }
  }
}
