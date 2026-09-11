package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BatchedLogBuffer}\
 *
 * <p>TODO: out of order log records gets correct oldest metric TODO: TEST a big line bigger than
 * the max bytes gets through TODO: test metrics using SimpleMeterRegistry
 */
public class BatchedLogBufferTest extends BillingTestBase {

  // *********************************************************
  // Offer - Producer side of the buffer
  // *********************************************************

  /** When the buffer reaches capacity calling offer() fails. Single producer thread. */
  @Test
  public void offerFailsAtCapacitySingleThread() {

    var fixture = defaultBufferFixture(false);
    var slice = Slice.to(BUFFER_CAPACITY);

    // send full capacity to the buffer, should all work
    fixture.assertOffer("offerFailsAtCapacitySingleThread() - prefill to capacity", slice);

    // Buffer should now be full, try to add one more
    fixture.assertBufferFull("offerFailsAtCapacitySingleThread()", BUFFER_CAPACITY + 1);
  }

  /** When the buffer reaches capacity calling offer() fails. Multiple producer threads. */
  @Test
  public void offerFailsAtCapacityMultiThread() {

    var fixture = defaultBufferFixture(false);
    var snapshot = BufferSnapshot.create(fixture);
    var slice = Slice.to(BUFFER_CAPACITY);

    // fill the buffer to capacity from 6 threads calling offer()
    // auto close will wait for tasks to finish in executor
    try (var pool = Executors.newFixedThreadPool(6)) {
      for (var record : slice.stream(fixture.logRecords()).toList()) {
        pool.submit(() -> fixture.buffer().offer(record));
      }
    }

    // check the change in the buffer is expected given the slice of source data
    snapshot.assertAll("offerFailsAtCapacityMultiThread()", slice, false);
    // Buffer should now be full, try to add one more
    fixture.assertBufferFull("offerFailsAtCapacityMultiThread()", BUFFER_CAPACITY + 1);
  }

  /**
   * Verify that when offered a LogRecord the buffer does not hold reference to the LogRecord and it
   * can be GC'd
   */
  @Test
  public void offerDoesNotHoldReferences() {

    var fixture = defaultBufferFixture(false);

    // do not use the records in the fixture, they are held in a list
    var record = new LogRecord(Level.INFO, "offerDoesNotHoldReferences()");
    var ref = new WeakReference<>(record);

    fixture.buffer().offer(record);
    record = null;

    // reference count for the object created for "record" above should now be zero
    // will timeout if the object is not GC'd and error
    await("offerDoesNotHoldReferences() - waiting for record to be GC'd")
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              System.gc();
              return ref.get() == null;
            });
  }

  @Test
  public void offerNullRecord() {
    var fixture = defaultBufferFixture(false);

    assertThatThrownBy(() -> fixture.buffer().offer(null))
        .as("offerNullRecord() null log record is an exception")
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void offerNullOrBlankMessage() {
    var fixture = defaultBufferFixture(false);

    var nullRecord = new LogRecord(Level.INFO, null);
    var blankRecord = new LogRecord(Level.INFO, " ");

    assertThatThrownBy(() -> fixture.buffer().offer(nullRecord))
        .as("offerNullOrBlankMessage() - null message is an error")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> fixture.buffer().offer(blankRecord))
        .as("offerNullOrBlankMessage() - blank message is an error")
        .isInstanceOf(IllegalArgumentException.class);
  }

  // *********************************************************
  // nextBatch - Consumer side of the buffer
  // *********************************************************

  /** When buffer is empty, there is no batch available. */
  @Test
  public void nextBatchEmptyBufferNoBatch() {

    // lock the clock, do not want it to auto advance for batch testing
    var fixture = defaultBufferFixture(true);

    assertThat(fixture.buffer().nextBatch(false))
        .as("nextBatchEmptyBufferNoBatch() - drainFully=false, no batch")
        .isNull();

    assertThat(fixture.buffer().nextBatch(true))
        .as("nextBatchEmptyBufferNoBatch() - drainFully=true, no batch")
        .isNull();
  }

  /** Properties of the returned batch object are as expected. */
  @Test
  public void nextBatchBatchProperties() {

    // lock the clock, do not want it to auto advance for batch testing
    var fixture = defaultBufferFixture(true);
    var slice = Slice.to(BUFFER_CAPACITY);

    // fill the buffer with all the records it will fit
    fixture.assertOffer("nextBatchBatchProperties()", slice);

    // keep taking batches and check their properties
    BatchedLogBuffer.Batch batch;
    Set<UUID> batchIds = new HashSet<>();
    while ((batch = fixture.buffer().nextBatch(true)) != null) {

      assertThat(batch.id())
          .as("nextBatchBatchProperties() - batch ID has not been seen")
          .satisfies(batchIds::add);

      assertThat(batch.toString())
          .as("nextBatchBatchProperties() - batch toString has values")
          .contains("id=" + batch.id())
          .contains("reason=" + batch.reason())
          .contains("size=" + batch.size())
          .contains("bytes=" + batch.bytes());
    }

    assertThat(fixture.buffer().isEmpty())
        .as("nextBatchBatchProperties() - drained buffer is empty")
        .isTrue();
  }

  /** Metadata (size etc) for the buffer is updated after a batch is returned. */
  @Test
  public void nextBatchMetaUpdatedAfterBatch() {

    // lock the clock, do not want it to auto advance for batch testing
    var fixture = defaultBufferFixture(true);
    var slice = Slice.to(MAX_BATCH_SIZE);

    // fill the buffer with 1 batch size and assert metadata
    fixture.assertOffer("nextBatchMetaUpdatedAfterBatch()", slice);

    // take 1 batch
    // we are only checking that the bookkeeping on the buffer changes, not checking
    // rules for batch selections, this is done by assertNextBatch()
    var batch1 = fixture.assertNextBatch("nextBatchMetaUpdatedAfterBatch() - 1st", false);

    // take a second batch and check again bookkeeping updated
    var batch2 = fixture.assertNextBatch("nextBatchMetaUpdatedAfterBatch() - 2nd", false);
  }

  /** Trigger a batch from the number of records added to buffer */
  @Test
  public void nextBatchTriggerMaxSize() {

    // change so the template is small so does not trigger max bytes
    // lock the clock, do not want it to auto advance for batch testing
    var fixture =
        Fixture.createFixture(
            MAX_BATCH_SIZE,
            MAX_BATCH_BYTES * 100, // big number so never batch because of bytes
            MAX_AGE,
            BUFFER_CAPACITY,
            NUM_RECORDS,
            LOG_LEVEL,
            "test-",
            true,
            false,
            false,
            false);

    // Fill to 1 less than max batch size, should be no batch
    var slice1 = Slice.to(MAX_BATCH_SIZE - 1);
    fixture.assertOffer("nextBatchMetaUpdatedAfterBatch()", slice1);
    var batch1 = fixture.buffer().nextBatch(false);
    assertThat(batch1).as("nextBatchTriggerMaxSize() - < MAX_BATCH_SIZE, no batch").isNull();

    // add one more record, should be a batch of MAX_BATCH_SIZE
    var slice2 = Slice.slice(MAX_BATCH_SIZE - 1, MAX_BATCH_SIZE);
    fixture.assertOffer("nextBatchMetaUpdatedAfterBatch()", slice2);
    var batch2 = fixture.assertNextBatch("nextBatchMetaUpdatedAfterBatch() - 2nd", false);

    assertThat(batch2.size())
        .as("nextBatchTriggerMaxSize() - 2nd batch is full batch size")
        .isEqualTo(MAX_BATCH_SIZE);

    assertThat(batch2.reason())
        .as(
            "nextBatchTriggerMaxSize() - 2nd batch because "
                + BatchedLogBuffer.BillingBatchReason.MAX_SIZE_EXCEEDED)
        .isEqualTo(BatchedLogBuffer.BillingBatchReason.MAX_SIZE_EXCEEDED);

    // add one more record, should be no more batches
    var slice3 = Slice.slice(MAX_BATCH_SIZE, MAX_BATCH_SIZE + 1);
    fixture.assertOffer("nextBatchMetaUpdatedAfterBatch() - 3rd", slice3);
    var batch3 = fixture.buffer().nextBatch(false);
    assertThat(batch3).as("nextBatchTriggerMaxSize() - 3rd - no batch").isNull();
  }

  /** Trigger a batch from the byte size in the buffer */
  @Test
  public void nextBatchTriggerMaxBytes() {

    // default fixture will only fit
    // the MAX_BATCH_BYTES_NUM_MESSAGES which is less than MAX_SIZE
    // lock the clock, do not want it to auto advance for batch testing
    var fixture = defaultBufferFixture(true);

    // Fill to 1 message less than max bytes size, should be no batch
    var slice1 = Slice.to(MAX_BATCH_BYTES_NUM_MESSAGES - 1);
    fixture.assertOffer("nextBatchTriggerMaxBytes()", slice1);
    var batch1 = fixture.buffer().nextBatch(false);
    assertThat(batch1).as("nextBatchTriggerMaxSize() - < MAX_BATCH_BYTES, no batch").isNull();

    // add one more , should be a batch of full batch bytes
    var slice2 = Slice.slice(MAX_BATCH_BYTES_NUM_MESSAGES - 1, MAX_BATCH_BYTES_NUM_MESSAGES);
    fixture.assertOffer("nextBatchTriggerMaxBytes()", slice2);
    var batch2 = fixture.assertNextBatch("nextBatchTriggerMaxBytes() - 2nd", false);

    // we know how many we put in there
    assertThat(batch2.bytes())
        .as("nextBatchTriggerMaxBytes() - 2nd batch byte size match")
        .isEqualTo(MAX_BATCH_BYTES_NUM_MESSAGES * MESSAGE_LENGTH_IN_BUFFER);

    assertThat(batch2.reason())
        .as(
            "nextBatchTriggerMaxBytes() - 2nd batch because "
                + BatchedLogBuffer.BillingBatchReason.MAX_BYTES_EXCEEDED)
        .isEqualTo(BatchedLogBuffer.BillingBatchReason.MAX_BYTES_EXCEEDED);

    // add one more, should be no more batches
    var slice3 = Slice.slice(MAX_BATCH_BYTES_NUM_MESSAGES, MAX_BATCH_BYTES_NUM_MESSAGES + 1);
    fixture.assertOffer("nextBatchTriggerMaxBytes() - 3rd", slice3);
    var batch3 = fixture.buffer().nextBatch(false);
    assertThat(batch3).as("nextBatchTriggerMaxBytes() - 3rd - no batch").isNull();
  }

  /** Trigger a batch from the maximum age of the first element in the buffer */
  @Test
  public void nextBatchTriggerMaxAge() {

    // lock the clock, do not want it to auto advance for batch testing
    // NOTE: WE ARE USING THE MOCK CLOCK IN THIS TEST, WE CONTROL TIME
    var fixture = defaultBufferFixture(true);

    // Add only 3 messages, we will not trip size or bytes tigger
    final int ADDED_RECORDS = 3;
    var slice1 = Slice.to(ADDED_RECORDS);
    fixture.assertOffer("nextBatchTriggerMaxAge()", slice1);

    // the clock has not moved, there should be no batch
    var batch1 = fixture.buffer().nextBatch(false);
    assertThat(batch1).as("nextBatchTriggerMaxAge() - clock as not moved, no batch").isNull();

    // Every LogRecord created in fixture has an instanceAt of 1 second after the previous
    // the first LogRecord has the same instanceAt as when the clock started.
    // so if we advance the clock to be MAX_AGE after when it started the only LogRecord that will
    // be too old is the first, the others are all 1+ seconds younger
    var newNow = fixture.clock().startedAt().plus(MAX_AGE);
    fixture.clock().setInstant(newNow);

    // The buffer should now think the time is "newNow"
    // Sanity check, before getting the batch check that only the first log record is MAX_AGE
    // checking all this junk did what I think
    int i = 0;
    var peekedBuffer = fixture.buffer().peekBuffer();
    for (var peekEntry : peekedBuffer) {
      var entryAge = fixture.buffer().entryAge(peekEntry);
      if (i == 0) {
        assertThat(entryAge)
            .as("nextBatchTriggerMaxAge() - clock moved, first entry should be MAX_AGE old")
            .isEqualTo(MAX_AGE);
      } else {
        assertThat(entryAge)
            .as(
                "nextBatchTriggerMaxAge() - clock moved, non first entry should be < MAX_AGE old. i: "
                    + i)
            .isLessThan(MAX_AGE);
      }
      i++;
    }

    // with the clock advanced the buffer should now trigger a batch because
    // MAX_AGE_EXCEEDED
    var batch2 = fixture.assertNextBatch("nextBatchTriggerMaxAge() - 2nd", false);

    assertThat(batch2.reason())
        .as(
            "nextBatchTriggerMaxAge() - 2nd batch because "
                + BatchedLogBuffer.BillingBatchReason.MAX_AGE_EXCEEDED)
        .isEqualTo(BatchedLogBuffer.BillingBatchReason.MAX_AGE_EXCEEDED);

    // should have drained all the messages, even if they were not too old
    assertThat(fixture.buffer().size())
        .as("nextBatchTriggerMaxAge() - 2nd batch buffer, size")
        .isEqualTo(0);
    assertThat(fixture.buffer().isEmpty())
        .as("nextBatchTriggerMaxAge() - 2nd batch buffer, isEmpty")
        .isTrue();
    assertThat(fixture.buffer().queuedBytes())
        .as("nextBatchTriggerMaxAge() - 2nd batch buffer, bytes")
        .isEqualTo(0);

    // sanity check, we should have ADDED_RECORDS entries in the batch
    // and the oldest should be the first one we created
    assertThat(batch2.size())
        .as("nextBatchTriggerMaxAge() - 2nd batch buffer, size expected")
        .isEqualTo(ADDED_RECORDS);
    assertThat(batch2.oldestEventAt())
        .as("nextBatchTriggerMaxAge() - 2nd batch buffer, oldest event expected")
        .isEqualTo(fixture.logRecords().getFirst().getInstant());

    // add one more record, should be no more batches
    var slice3 = Slice.slice(ADDED_RECORDS, ADDED_RECORDS + 1);
    fixture.assertOffer("nextBatchTriggerMaxAge() - 3rd", slice3);
    var batch3 = fixture.buffer().nextBatch(false);
    assertThat(batch3).as("nextBatchTriggerMaxAge() - 3rd - no batch").isNull();
  }

  /**
   * Trigger a batch because drainFully=true so we want everything from it regardless of size,
   * bytes, age
   */
  @Test
  public void nextBatchTriggerDrain() {

    // lock the clock, do not want it to auto advance for batch testing
    var fixture = defaultBufferFixture(true);

    // Fill so we have 1 full batch and 1 partial batch
    var PARTIAL_BATCH_SIZE = 10;
    var slice1 = Slice.to(MAX_BATCH_BYTES_NUM_MESSAGES + PARTIAL_BATCH_SIZE);
    fixture.assertOffer("nextBatchTriggerDrain()", slice1);

    // 1st - drainFully -  should get a full batch
    var batch1 = fixture.assertNextBatch("nextBatchTriggerDrain() - 1st - full batch", true);
    assertThat(batch1.reason())
        .as(
            "nextBatchTriggerDrain() - 1st - reason is "
                + BatchedLogBuffer.BillingBatchReason.DRAINING)
        .isEqualTo(BatchedLogBuffer.BillingBatchReason.DRAINING);
    assertThat(batch1.size())
        .as("nextBatchTriggerDrain() - 1st - full batch, size")
        .isEqualTo(MAX_BATCH_BYTES_NUM_MESSAGES);

    // 2nd - drainFully - should get a partial batch
    var batch2 = fixture.assertNextBatch("nextBatchTriggerDrain() - 2nd - partial batch", true);
    assertThat(batch2.reason())
        .as(
            "nextBatchTriggerDrain() - 2nd - reason is "
                + BatchedLogBuffer.BillingBatchReason.DRAINING)
        .isEqualTo(BatchedLogBuffer.BillingBatchReason.DRAINING);
    assertThat(batch2.size())
        .as("nextBatchTriggerDrain() - 2nd - partial batch, size")
        .isEqualTo(PARTIAL_BATCH_SIZE);

    // 3rs - drainFully - no more batch
    var batch3 = fixture.buffer().nextBatch(true);
    assertThat(batch3).as("nextBatchTriggerMaxBytes() - 3rd - no batch").isNull();
  }

  /**
   * Multiple producers sending to the buffer, and one consumer reading from it concurrently.
   *
   * <p><b>NOTE:</b> ttest takes 7 or 8 seconds, if you change the sleep time it may mean there are
   * no batches collected after shutdown because producers go fast
   */
  @Test
  public void multiThreadedProducerConsumer() {

    var fixture = defaultBufferFixture(false);

    // Setup a Consumer thread, it will keep running until we set consumerShutdown
    var normalBatches = new ArrayList<BatchedLogBuffer.Batch>();
    var shutdownBatches = new ArrayList<BatchedLogBuffer.Batch>();
    var consumerShutdown = new AtomicBoolean(false);
    var consumerExecutor =
        Executors.newSingleThreadExecutor(Thread.ofPlatform().name("consumer-", 0).factory());

    var consumerFuture =
        consumerExecutor.submit(
            () -> {
              // this is the consumer in normal operations, read batches, if none sleep, read again
              while (!consumerShutdown.get()) {
                BatchedLogBuffer.Batch consumerNormalBatch;
                // drainFully=false - because not trying to shutdown
                while ((consumerNormalBatch = fixture.buffer().nextBatch(false)) != null) {
                  normalBatches.add(consumerNormalBatch);
                  // fake that we do some work with the batch, e.g. upload it
                  threadSleep(50);
                }
                // fake the sleep between waking up to check for a batch
                threadSleep(50);
              }

              // now into the shutdown mode, so drainFully=true to empty the buffer
              BatchedLogBuffer.Batch consumerShutdownBatch;
              while ((consumerShutdownBatch = fixture.buffer().nextBatch(true)) != null) {
                shutdownBatches.add(consumerShutdownBatch);
                // fake that we do some work with the batch, e.g. upload it
                threadSleep(50);
              }
            });
    // the consumer is running async looping waiting for batches from the buffer

    // Now setup producers to send data for it, we are going to send all the records we
    // created, this will be more than the buffer capacity.
    var NUM_PRODUCER_THREADS = 4;
    var slice = Slice.to(NUM_RECORDS);
    var threadFactory = Thread.ofPlatform().name("producer-", 0).factory();
    var producedCount = new AtomicLong();
    // we want to pause all producers half way through producing so we can
    // shutdown the consumer and then produce the remaining records
    var producerHalfwayLatch = new CountDownLatch(NUM_PRODUCER_THREADS);

    try (var pool = Executors.newFixedThreadPool(NUM_PRODUCER_THREADS, threadFactory)) {
      for (var record : slice.stream(fixture.logRecords()).toList()) {

        // Append the thread name to the log record for debugging
        // this will break the config at top of class about how many messages per batch
        pool.submit(
            () -> {
              record.setMessage(
                  record.getMessage() + " - THREAD " + Thread.currentThread().getName());
              fixture.buffer().offer(record);

              if ((producedCount.incrementAndGet() >= (slice.size() / 2))
                  && (!consumerShutdown.get())) {
                // this thread got at least half way, mark that and wait for all others
                // to get this far
                producerHalfwayLatch.countDown();
                waitOnLatch(producerHalfwayLatch);

                // Signal the consumer to shut down, next time it wakes it will start using
                // drainFully
                consumerShutdown.set(true);
              } else {
                // Fake that we are doing other things, do not do if we paused cause we want to
                // get back to producing ASAP
                threadSleep(25);
              }
            });
      }
    } // try, will block waiting for threads to finish when closing Executor

    // wait for consumer to finish
    try {
      consumerFuture.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    } finally {
      // close consumer thread pool
      consumerExecutor.close();
    }

    // Now we can check consumer got all the data
    // all the batches from normal processing should be either max size or bytes
    for (var batch : normalBatches) {

      assertThat(batch.reason())
          .as("multiThreadedProducerConsumer() - normal batch reason is size or bytes")
          .isIn(
              List.of(
                  BatchedLogBuffer.BillingBatchReason.MAX_SIZE_EXCEEDED,
                  BatchedLogBuffer.BillingBatchReason.MAX_BYTES_EXCEEDED));
      // sanity check that the messages in the batch came from a producer thread.
      for (var line : batch.lines()) {
        assertThat(line)
            .as("multiThreadedProducerConsumer() - normal batch line created by producer thread.")
            .contains("THREAD producer-");
      }
    }

    // all the batches from shutdown processing must be due to draining
    for (var batch : shutdownBatches) {

      assertThat(batch.reason())
          .as("multiThreadedProducerConsumer() - normal batch reason is draining")
          .isEqualTo(BatchedLogBuffer.BillingBatchReason.DRAINING);
      // sanity check that the messages in the batch came from a producer thread.
      for (var line : batch.lines()) {
        assertThat(line)
            .as("multiThreadedProducerConsumer() - shutdown batch line created by producer thread.")
            .contains("THREAD producer-");
      }
    }

    // total lines from normal and shutdown must be total from producers
    var totalBatchLines =
        Stream.concat(shutdownBatches.stream(), normalBatches.stream())
            .mapToInt(BatchedLogBuffer.Batch::size)
            .sum();
    assertThat(totalBatchLines)
        .as(
            "multiThreadedProducerConsumer() - lines from batches same number as produced: "
                + producedCount.get())
        .isEqualTo(producedCount.get());

    // sanity check - did every producer thread produce at least one log record ?
    // log message will look like: "Total of 25 chars    059 - THREAD producer-1"
    for (int i = 0; i < NUM_PRODUCER_THREADS; i++) {
      var threadSuffix = "- THREAD producer-" + i;
      var found =
          Stream.concat(shutdownBatches.stream(), normalBatches.stream())
              .flatMap(batch -> batch.lines().stream())
              .anyMatch(line -> line.endsWith(threadSuffix));
      assertThat(found).as("Producer thread created record, thread:" + threadSuffix).isTrue();
    }
  }

  // *********************************************************
  // Basic object testing
  // *********************************************************

  @Test
  public void testConstructor() {

    var metrics = mock(BatchedLogBufferMetrics.class);
    assertThatThrownBy(
            () -> new BatchedLogBuffer(0, 1, Duration.ofSeconds(1), 10, metrics),
            "maxBatchSize < 1")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new BatchedLogBuffer(1, 0, Duration.ofSeconds(1), 10, metrics),
            "maxBatchBytes < 1")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new BatchedLogBuffer(1, 1, Duration.ofSeconds(-1), 10, metrics),
            "maxBatchAge < 1")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new BatchedLogBuffer(1, 1, Duration.ofSeconds(0), 10, metrics), "maxBatchAge = 0")
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new BatchedLogBuffer(1, 1, Duration.ofSeconds(1), 0, metrics), "queueCapacity =0")
        .isInstanceOf(IllegalArgumentException.class);

    clearInvocations(metrics);
    var buffer = new BatchedLogBuffer(1, 2, Duration.ofSeconds(1), 10, metrics);
    verify(metrics, times(1).description("buffer registers with metrics")).registerBuffer(any());

    assertThat(buffer.toString())
        .as("buffer toString has correct values")
        .startsWith(classSimpleName(buffer))
        .contains("maxBatchSize=1")
        .contains("maxBatchBytes=2")
        .contains("maxBatchAge=PT1S")
        .contains("size=0");
  }
}
