package io.stargate.sgv2.jsonapi.service.billing;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogBufferMetrics;
import io.stargate.sgv2.jsonapi.metrics.BatchedLogUploaderMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogRecord;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

/**
 * Buffer for {@link LogRecord} that batches them according to the configuration.
 * <p>
 * See {@link #BatchedLogBuffer(int, long, Duration, int, BatchedLogUploaderMetrics)} for the config.
 * </p>
 * <p>
 * There are two uses of this class, producers and consumers.
 * <ul>
 *     <li>Producers - call {@link #offer(LogRecord)} to add the log record to the buffer.</li>
 *     <li>Consumers - call {@link #nextBatch(boolean)} to get the next batch to send if there is a
 *     full batch.</li>
 * </ul>
 *
 * The buffer is designed to handle these as concurrent calls from different threads, and tracks
 * metrics for its use.
 * </p>
 */
public class BatchedLogBuffer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BatchedLogBuffer.class);

  private final int maxBatchSize;
  private final long maxBytes;
  private final Duration maxAge;

  private final BlockingQueue<Entry> queue;
  private final BatchedLogBufferMetrics metrics;

  private final AtomicLong queuedBytes = new AtomicLong(0);

  /**
   * Creates a new instance of the buffer.
   *
   * @param maxBatchSize Maximum number of log records in a batch, when the buffer has more than this many
   *                     entries a new batch is made available which will contain no more than this many lines.
   * @param maxBatchBytes Maximum numbers of bytes in a batch, when the buffer has more than this many entries
   *                      a new batch is made available which may contain more than this many bytes. The batch will
   *                      have many maxBatchBytes if there is a single log record that is bigger.
   * @param maxAge Maximum age any log record should have in the buffer before a new batch is available.
   * @param queueCapacity Total number of log records to buffer.
   * @param metrics Metrics recording object.
   */
  public BatchedLogBuffer(int maxBatchSize, long maxBatchBytes, Duration maxAge, int queueCapacity, BatchedLogBufferMetrics metrics) {

    if (maxBatchSize < 1) {
      throw new IllegalArgumentException("maxBatchSize must be >= 1, got: " + maxBatchSize );
    }
    if (maxBatchBytes < 1) {
      throw new IllegalArgumentException("maxBatchBytes must be >= 1, got: " + maxBatchBytes );
    }
    if (queueCapacity < 1) {
      throw new IllegalArgumentException("queueCapacity must be >= 1, got: " + queueCapacity);
    }
    if (maxAge == null || maxAge.isNegative() || maxAge.isZero()) {
      throw new IllegalArgumentException("maxAge must be positive, got: " + maxAge);
    }
    this.maxBatchSize = maxBatchSize;
    this.maxBytes = maxBatchBytes;
    this.maxAge = maxAge;

    this.metrics = Objects.requireNonNull(metrics,  "billingMetrics must not be null");
    this.metrics.registerBuffer(this);
    // must be concurrent to handle multiple threads
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
  }

  /**
   * Appends the LogRecord to the buffer if the buffer has capacity.
   *
   *  <p>
   *  <b>NOTE:</b> because this is used for billing information if the record is
   *  null or has an empty message an exception is thrown rather than
   *  silently dropping it. We expect this situation to be an exception and it should fail.
   *  </p>
   * @param record {@link LogRecord} to add to the buffer.
   * @return true if the record was added to be buffer, false if the buffer did not have capacity. NOTE:
   * this is different to the param check for record, the buffer filling is tracked as metric but no error.
   *
   */
  public boolean offer(LogRecord record) {

    Objects.requireNonNull(record, "record must not be null");

    var logLine = record.getMessage();
    if (logLine == null || logLine.isBlank()) {
      throw new IllegalArgumentException("record.getMessage() must not be null or blank");
    }
    var newEntry = new Entry(record.getInstant(), logLine);

    metrics.offered.increment();
    if (!queue.offer(newEntry)) {
      // Bounded buffer full: drop and count
      LOGGER.debug("offer() - buffer full, dropping new entry: {}", newEntry);
      metrics.dropped.increment();
      return false;
    }

    queuedBytes.addAndGet(newEntry.lineBytes());
    return true;
  }

  /**
   * Returns the next batch of messages from the {@link LogRecord}'s added to the buffer,
   * if one is available.
   * <p>
   * Designed to be called from different threads than those producing LogRecord's.
   * </p>
   *
   * @param drainFully when True a new batch is created without checking the configured
   *                   rules, use this when draining the buffer and there may only
   *                   be a partial batch.
   * @return A new {@link Batch} of log messages all of which have been removed from the
   *        buffer, or <code>null</code> if there is no next batch.
   * */
  public Batch nextBatch(boolean drainFully) {

    var batchReason = decideNextBatch(drainFully);
    if (batchReason == null) {
      return null;
    }

    List<String> lines = new ArrayList<>(maxBatchSize);
    Instant oldestEventAt = null;
    long batchBytes = 0;
    Entry entry;

    // No matter why we started we create a full batch, e.g. we could start because the oldest
    // entry is past maxAge, but we still fill the batch.
    while (lines.size() < maxBatchSize && batchBytes < maxBytes && (entry = queue.poll()) != null) {

      if (oldestEventAt == null || entry.eventAt().isBefore(oldestEventAt)) {
        oldestEventAt = entry.eventAt();
      }

      lines.add(entry.line());
      var lineBytes = entry.lineBytes();
      queuedBytes.addAndGet(-lineBytes);
      batchBytes += lineBytes;
    }

    // sanity check, in case of concurrent calls
    if (lines.isEmpty()){
      return null;
    }

    LOGGER.info("nextBatch() - next batch of log buffer, reason:{}, lines.size:{}, batchBytes:{}, oldestEventAt: {}",
            batchReason, lines.size(), batchBytes, oldestEventAt);
    return new Batch(batchReason, lines, batchBytes, oldestEventAt);
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

  public Duration headEntryAge() {
    var head = queue.peek();
    return head == null ? Duration.ZERO : Duration.between(head.eventAt(), Instant.now());
  }

  private BillingBatchReason decideNextBatch(boolean drainFully) {

    if (drainFully){
      return BillingBatchReason.DRAINING;
    }
    if (queue.size() >= maxBatchSize) {
      return BillingBatchReason.MAX_BATCH_SIZE_EXCEEDED;
    }
    if (queuedBytes.get() > maxBytes) {
      return BillingBatchReason.MAX_BYTES_EXCEEDED;
    }
    if (headEntryAge().compareTo(maxAge) >= 0){
      return BillingBatchReason.MAX_AGE_EXCEEDED;
    }
    return null;
  }

  public enum BillingBatchReason {
    DRAINING,
    MAX_BATCH_SIZE_EXCEEDED,
    MAX_BYTES_EXCEEDED,
    MAX_AGE_EXCEEDED
  }

  /**
   *
   */
  public static final class Batch {

    private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

    private final UUID batchId = UUID_V7_GENERATOR.generate();
    private final BillingBatchReason batchReason;
    private final List<String> lines;
    private final long batchBytes;
    private final Instant oldestEventAt;

    public Batch(BillingBatchReason batchReason, List<String> lines, long batchBytes, Instant oldestEventAt) {
      this.batchReason = batchReason;
      this.lines = Collections.unmodifiableList(lines);
      this.batchBytes = batchBytes;
      this.oldestEventAt = oldestEventAt;
    }

    public UUID id() {
      return batchId;
    }

    public BillingBatchReason reason() {
      return batchReason;
    }

    public List<String> lines() {
      return lines;
    }

    public Instant oldestEventAt() {
      return oldestEventAt;
    }

    public Duration oldestEventAtDuration() {
      return Duration.between(oldestEventAt(), Instant.now());
    }

    public int size() {
      return lines.size();
    }

    public long batchBytes() {
      return batchBytes;
    }

    public String description() {
      return "id:%s, reason:%s, oldestEventAt:%s, size:%s, batchBytes:%s".formatted(
              batchId, batchReason, oldestEventAt, size(),  batchBytes
      );
    }
  }

  /**
   * Holder for the billing event lines we get called with.
   * @param eventAt When the event happened
   * @param line The billing event line to record
   */
  private record Entry(Instant eventAt, String line) {

    /**
     * Gets the length of the line in bytes,
     * <p>
     * Kind of a hack, we are counting unicode code points and calling that 1 byte. Should work
     * for ascii text, will undercount if there is non ascii chars but everthing in billing should be ascii
     */
    public int lineBytes() {
      return line.length() + 1; // +1 is for a newline
    }
  }
}
