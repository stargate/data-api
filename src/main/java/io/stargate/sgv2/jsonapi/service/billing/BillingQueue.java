package io.stargate.sgv2.jsonapi.service.billing;

import com.google.common.annotations.VisibleForTesting;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded in-memory buffer that owns the batching policy of the billing S3 export: it decides when
 * a batch is sealed ({@code maxEvents} lines or {@code maxBytes} UTF-8 NDJSON bytes) and hands out
 * drained {@link Batch}es. A batch may exceed {@code maxBytes} by one whole line; lines are never
 * split.
 */
public class BillingQueue {

  private final BlockingQueue<Entry> queue;

  // Approximate buffered  bytes (see lineBytes);
  private final AtomicLong queuedBytes = new AtomicLong(0);
  private final int maxBatchSize;
  private final long maxBytes;
  private final Duration maxAge;

  public BillingQueue(int maxBatchSize, long maxBatchBytes, Duration maxAge, int queueCapacity) {

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

    // must be concurrent to handle multiple threads
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public int size() {
    return queue.size();
  }

  /** Buffered-bytes counter, exposed for accounting assertions only (no production caller). */
  @VisibleForTesting
  long queuedBytes() {
    return queuedBytes.get();
  }

  /**
   * Appends a line to the billing queue
   *
   * @param eventAt when the event was logged
   * @param line String for the JSON billing blob
   * @return true if the billing line was added to the quee for publishing, false otherwise which
   *        means the queue has reached capacity and we can no longer buffer events
   *
   * // AI BELOW
   * Buffers one line, or returns {@code false} when the capacity bound is hit.
   *
   * @param eventAt when the event was logged; carried through to {@link Batch#oldestEventAt()}
   */
  public boolean offer(Instant eventAt, String line) {

    var newEntry = new Entry(eventAt, line);
    if (!queue.offer(newEntry)) {
      return false;
    }
    queuedBytes.addAndGet(newEntry.lineBytes());
    return true;
  }

  private Duration oldestEntry() {
    var head = queue.peek();

    return head == null ? Duration.ZERO : Duration.between(head.eventAt(), Instant.now());
  }

  /**
   * Tests if we should start a new batch
   * */
  private BillingBatchReason startNextBatch() {

    if (queue.size() >= maxBatchSize) {
      return BillingBatchReason.MAX_BATCH_SIZE_EXCEEDED
    }
    if (queuedBytes.get() > maxBytes) {
      return BillingBatchReason.MAX_BYTES_EXCEEDED;
    }
    if (oldestEntry().compareTo(maxAge) >= 0){
      return BillingBatchReason.MAX_AGE_EXCEEDED;
    }
    return null;
  }

  /**
   *
   * Assumes we are not running async
   * OLD BELOW:
   *
   * Removes and returns up to one sealed batch
   * */
  public Batch maybeDrain() {

    var batchReason = startNextBatch();
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

    return new Batch(batchReason, lines, oldestEventAt);
  }


  public enum BillingBatchReason {
    MAX_BATCH_SIZE_EXCEEDED,
    MAX_BYTES_EXCEEDED,
    MAX_AGE_EXCEEDED
  }

  /**
   * OLD BELOW
   *
   * One drained, sealed batch. {@code oldestEventAt} is the minimum event time across {@code lines}
   * — queue order is enqueue order, not event-time order, under concurrent publish.
   */
  public record Batch(BillingBatchReason batchReason, List<String> lines, Instant oldestEventAt) {

    public Batch{
      lines = Collections.unmodifiableList(lines);
    }

    public int size(){
      return lines.size();
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
