package io.stargate.sgv2.jsonapi.service.billing;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffer for {@link LogRecord} that batches them according to the configuration when created.
 *
 * <p>There are two users of this class:
 *
 * <ul>
 *   <li>Producers - call {@link #offer(LogRecord)} to add the log record to the buffer. There can
 *       be many consumers form different threads.
 *   <li>Consumers - call {@link #nextBatch(boolean)} to get the next batch if available. There
 *       should be only 1 consumer calling at a time, caller is responsible for this.
 * </ul>
 *
 * The buffer is designed to handle these as concurrent calls from different threads, and tracks
 * metrics for its use.
 */
public class BatchedLogBuffer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedLogBuffer.class);

  @VisibleForTesting static final Clock DEFAULT_CLOCK = Clock.systemUTC();

  private final int maxBatchSize;
  private final long maxBatchBytes;
  private final Duration maxBatchAge;

  // don't really need to keep this, just here for debugging
  private final int capacity;

  // Clock we use to get the current time when checking age.
  // so it can be controlled when testing
  private final Clock clock;

  private final BlockingQueue<Entry> queue;
  private final AtomicLong queuedBytes = new AtomicLong(0);
  private final BatchedLogBufferMetrics metrics;

  /** See {@link #BatchedLogBuffer(int, long, Duration, int, BatchedLogBufferMetrics, Clock)} */
  BatchedLogBuffer(
      int maxBatchSize,
      long maxBatchBytes,
      Duration maxBatchAge,
      int capacity,
      BatchedLogBufferMetrics metrics) {
    this(maxBatchSize, maxBatchBytes, maxBatchAge, capacity, metrics, DEFAULT_CLOCK);
  }

  /**
   * Creates a new instance of the buffer with configuration.
   *
   * <p><b>NOTE:</b> this ctor is for use in testing when the Clock is overridden, use the other
   * ctor in regular code.
   *
   * @param maxBatchSize Maximum number of log records in a batch, when the buffer has more than
   *     this many entries a new batch is made available which will contain no more than this many
   *     lines. Batch maybe smaller if maxBatchBytes is hit.
   * @param maxBatchBytes Maximum numbers of bytes in a batch, when the buffer has more than this
   *     many entries a new batch is made available which may contain more than this many bytes. The
   *     batch will have many maxBatchBytes if there is a single log record that is bigger.
   * @param maxBatchAge Maximum age the head log record should have in the buffer before a new batch
   *     is available. When a batch is triggered from max age the batch is filled, even if the other
   *     messages have not reached their max age.
   * @param capacity Total number of log records to buffer. Beyond this called to {@link
   *     #offer(LogRecord)} will fail to add the message.
   * @param metrics Metrics recording object.
   * @param clock The {@link Clock} implementation to use when checking the age of a message, this
   *     should only be overridden in testing. DO NOT USE IN CODE. If null uses {@link
   *     #DEFAULT_CLOCK}
   */
  @VisibleForTesting
  BatchedLogBuffer(
      int maxBatchSize,
      long maxBatchBytes,
      Duration maxBatchAge,
      int capacity,
      BatchedLogBufferMetrics metrics,
      Clock clock) {

    if (maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be >= 1, got: " + maxBatchSize);
    }
    if (maxBatchBytes < 1) {
      throw new IllegalArgumentException("maxBatchBytes must be >= 1, got: " + maxBatchBytes);
    }
    if (maxBatchAge == null || maxBatchAge.isNegative() || maxBatchAge.isZero()) {
      throw new IllegalArgumentException("maxAge must be positive, got: " + maxBatchAge);
    }

    this.maxBatchSize = maxBatchSize;
    this.maxBatchBytes = maxBatchBytes;
    this.maxBatchAge = maxBatchAge;
    this.capacity = capacity;
    this.metrics = Objects.requireNonNull(metrics, "billingMetrics must not be null");

    this.clock = clock == null ? DEFAULT_CLOCK : clock;
    if (this.clock != DEFAULT_CLOCK) {
      LOGGER.warn(
          "BatchedLogBuffer - WARNING - CONFIGURED TO USE A CUSTOM CLOCK, DO NOT USE IN PRODUCTION.");
    }
    // must be concurrent to handle multiple threads
    this.queue = new ArrayBlockingQueue<>(capacity);

    // just to be safe, register after queue created incase metrics are scrapped
    this.metrics.registerBuffer(this);
  }

  /**
   * Appends the LogRecord to the buffer if the buffer has capacity.
   *
   * <p><b>NOTE:</b> because this is used for billing information if the record is null or has an
   * empty message an exception is thrown rather than silently dropping it. We expect this situation
   * to be an exception and it should fail.
   *
   * @param record {@link LogRecord} to add to the buffer.
   * @return true if the record was added to be buffer, false if the buffer did not have capacity.
   */
  public boolean offer(LogRecord record) {

    Objects.requireNonNull(record, "record must not be null");

    var logLine = record.getMessage();
    if (logLine == null || logLine.isBlank()) {
      throw new IllegalArgumentException("record.getMessage() must not be null or blank");
    }
    var newEntry = new Entry(record.getInstant(), logLine);

    metrics.offered();
    if (!queue.offer(newEntry)) {
      // Bounded buffer full, drop and count
      LOGGER.debug("offer() - buffer full, dropping new entry: {}", newEntry);
      metrics.dropped();
      return false;
    }

    queuedBytes.addAndGet(newEntry.lineBytes());
    return true;
  }

  /**
   * Returns the next batch of messages from the {@link LogRecord}'s added to the buffer, if one is
   * available.
   *
   * <p>Designed to be called from different threads than the producers called {@link
   * #offer(LogRecord)}
   *
   * @param drainFully when True a new batch is created without checking the configured rules, use
   *     this when draining the buffer and there may only be a partial batch.
   * @return A new {@link Batch} of log messages all of which have been removed from the buffer, or
   *     <code>null</code> if there is no next batch.
   */
  public Batch nextBatch(boolean drainFully) {

    var batchReason = decideNextBatch(drainFully);
    if (batchReason == null) {
      return null;
    }

    List<String> batchLines = new ArrayList<>(maxBatchSize);
    Instant oldestEventAt = null;
    long batchBytes = 0;
    Entry peeked;

    // No matter why we started we create a full batch, e.g. we could start because the oldest
    // entry is past maxAge, but we still fill the batch.
    while (batchLines.size() < maxBatchSize && ((peeked = queue.peek()) != null)) {

      var lineBytes = peeked.lineBytes();
      if (batchBytes + lineBytes > maxBatchBytes && !batchLines.isEmpty()) {
        // adding the next line will be too many bytes, we can only do this if the batch
        // is empty, so a single big message can be put into a batch and not block everyone else
        // break out of here.
        break;
      }

      // OK to remove entry from buffer and add to batch
      // there is only this thread as a consumer, no race condition
      var polled = queue.poll();
      // sanity check
      if (polled != peeked) {
        throw new IllegalStateException(
            "nextBatch() - peeked entry is not same object as polled entry");
      }
      if (oldestEventAt == null || polled.eventAt().isBefore(oldestEventAt)) {
        oldestEventAt = polled.eventAt();
      }
      batchLines.add(polled.line());
      queuedBytes.addAndGet(-lineBytes);
      batchBytes += lineBytes;
    }

    if (batchLines.isEmpty() && !queue.isEmpty()) {
      // sanity check
      // there is messages in the queue, but we did not put any in the batch, something wrong
      // but it could be a race condition - things may be added after the loop finish
      // so do not throw, just log
      LOGGER.warn(
          "nextBatch() - did not add any lines for next batch, but queue is not empty. May be logic bug or expected race condition. queue.size:{}",
          queue.size());
      return null;
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "nextBatch() - next batch created, reason:{}, batchLines.size:{}, batchBytes:{}, oldestEventAt: {}",
          batchReason,
          batchLines.size(),
          batchBytes,
          oldestEventAt);
    }
    return new Batch(batchReason, batchLines, batchBytes, oldestEventAt, clock);
  }

  /** Gets a copy of the contents of the buffer in a new array list, for testing. */
  @VisibleForTesting
  List<Entry> peekBuffer() {
    return new ArrayList<>(queue);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public int size() {
    return queue.size();
  }

  public long queuedBytes() {
    return queuedBytes.get();
  }

  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  /**
   * Gets the age of the item at the head of the buffer.
   *
   * <p>Age is determined by the clock used to create the buffer.
   *
   * @return age of the head item in the buffer, or null if no items in the buffer.
   */
  public Duration headEntryAge() {
    return entryAge(queue.peek());
  }

  @VisibleForTesting
  Duration entryAge(Entry entry) {
    return entry == null ? Duration.ZERO : Duration.between(entry.eventAt(), clock.instant());
  }

  private BillingBatchReason decideNextBatch(boolean drainFully) {

    BillingBatchReason decision;
    if (queue.isEmpty()) {
      decision = null;
    } else if (drainFully) {
      decision = BillingBatchReason.DRAINING;
    } else if (queue.size() >= maxBatchSize) {
      decision = BillingBatchReason.MAX_SIZE_EXCEEDED;
    } else if (queuedBytes.get() >= maxBatchBytes) {
      decision = BillingBatchReason.MAX_BYTES_EXCEEDED;
    } else if (headEntryAge().compareTo(maxBatchAge) >= 0) {
      decision = BillingBatchReason.MAX_AGE_EXCEEDED;
    } else {
      decision = null;
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("decideNextBatch() - drainFully:{} , decision:{}", drainFully, decision);
    }
    return decision;
  }

  @Override
  public String toString() {
    return new StringBuilder(classSimpleName(this) + "{")
        .append("maxBatchSize=")
        .append(maxBatchSize)
        .append(", maxBatchBytes=")
        .append(maxBatchBytes)
        .append(", maxBatchAge=")
        .append(maxBatchAge)
        .append(", size=")
        .append(size())
        .append("}")
        .toString();
  }

  /**
   * The reason a batch was created by the buffer.
   *
   * <p>...
   */
  public enum BillingBatchReason {
    DRAINING,
    MAX_SIZE_EXCEEDED,
    MAX_BYTES_EXCEEDED,
    MAX_AGE_EXCEEDED
  }

  /**
   * A batch of log messages created by the buffer.
   *
   * <p>See {@link BatchedLogBuffer#nextBatch(boolean)}
   */
  public static final class Batch {

    private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

    private final UUID id = UUID_V7_GENERATOR.generate();
    private final BillingBatchReason reason;
    private final List<String> lines;
    private final long bytes;
    private final Instant oldestEventAt;
    private final Clock clock;

    Batch(BillingBatchReason reason, List<String> lines, long bytes, Instant oldestEventAt) {
      this(reason, lines, bytes, oldestEventAt, DEFAULT_CLOCK);
    }

    private Batch(
        BillingBatchReason reason,
        List<String> lines,
        long bytes,
        Instant oldestEventAt,
        Clock clock) {
      this.reason = Objects.requireNonNull(reason, "reason must not be null");
      this.lines =
          Collections.unmodifiableList(Objects.requireNonNull(lines, "lines must not be null"));
      this.bytes = bytes;
      this.oldestEventAt = Objects.requireNonNull(oldestEventAt, "oldestEventAt must not be null");
      this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public UUID id() {
      return id;
    }

    public BillingBatchReason reason() {
      return reason;
    }

    /**
     * @return Unmodifiable list of the log lines in this buffer
     */
    public List<String> lines() {
      return lines;
    }

    public Instant oldestEventAt() {
      return oldestEventAt;
    }

    public Duration oldestEventAtDuration() {
      // using the outer buffers clock, so any tests that change the clock
      // get a consistent results
      return Duration.between(oldestEventAt(), clock.instant());
    }

    public int size() {
      return lines.size();
    }

    public long bytes() {
      return bytes;
    }

    @Override
    public String toString() {
      return new StringBuilder(classSimpleName(this) + "{")
          .append("id=")
          .append(id)
          .append(", reason=")
          .append(reason)
          .append(", oldestEventAt=")
          .append(oldestEventAt)
          .append(", size=")
          .append(size())
          .append(", bytes=")
          .append(bytes())
          .append("}")
          .toString();
    }
  }

  /**
   * An entry in a batch, this is one log message passed to the buffer.
   *
   * @param eventAt When the log event happened
   * @param line The log message
   */
  public record Entry(Instant eventAt, String line) {

    /**
     * Gets the length of the line in bytes,
     *
     * <p>Kind of a hack, we are counting Unicode code points and calling that 1 byte. Should work
     * for ascii text, will undercount if there is non ASCII chars but everything in billing should
     * be ascii</b>
     *
     * @return length of the line in bytes, included a carriage return for `\n`
     */
    public int lineBytes() {
      return lineBytes(line);
    }

    /**
     * @return length of the line in bytes, included a carriage return for `\n`
     */
    @VisibleForTesting
    static int lineBytes(String line) {
      return line.length() + 1;
    }
  }
}
