package io.stargate.sgv2.jsonapi.service.provider;

import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.ArrayList;
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
public final class BillingQueue {

  private final BlockingQueue<Entry> queue;
  // Approximate buffered NDJSON bytes (see lineBytes);
  private final AtomicLong queuedBytes = new AtomicLong(0);
  private final int batchSize;
  private final long maxBytes;

  public BillingQueue(int maxEvents, long maxBytes, int queueCapacity) {
    if (maxEvents < 1) {
      throw new IllegalArgumentException(
          "stargate.jsonapi.billing.s3.max-events must be >= 1 (was " + maxEvents + ")");
    }
    if (maxBytes < 1) {
      throw new IllegalArgumentException(
          "stargate.jsonapi.billing.s3.max-bytes must be >= 1 (was " + maxBytes + ")");
    }
    if (queueCapacity < 1) {
      throw new IllegalArgumentException(
          "stargate.jsonapi.billing.s3.queue-capacity must be >= 1 (was " + queueCapacity + ")");
    }
    this.batchSize = maxEvents;
    this.maxBytes = maxBytes;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
  }

  /**
   * Buffers one line, or returns {@code false} when the capacity bound is hit.
   *
   * @param eventAt when the event was logged; carried through to {@link Batch#oldestEventAt()}
   */
  public boolean offer(Instant eventAt, String line) {
    if (!queue.offer(new Entry(eventAt, line))) {
      return false;
    }
    queuedBytes.addAndGet(lineBytes(line));
    return true;
  }

  /** True once a seal is reached: a full batch by line count, or {@code maxBytes} buffered. */
  public boolean shouldFlush() {
    return queue.size() >= batchSize || queuedBytes.get() >= maxBytes;
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

  /** Removes and returns up to one sealed batch (possibly partial, possibly {@code EMPTY}). */
  public Batch drain() {
    List<String> lines = new ArrayList<>(batchSize);
    Instant oldestEventAt = null;
    long bytes = 0;
    Entry entry;
    while (lines.size() < batchSize && bytes < maxBytes && (entry = queue.poll()) != null) {
      if (oldestEventAt == null || entry.eventAt().isBefore(oldestEventAt)) {
        oldestEventAt = entry.eventAt();
      }
      lines.add(entry.line());
      bytes += lineBytes(entry.line());
    }
    queuedBytes.addAndGet(-bytes);
    return oldestEventAt == null ? Batch.EMPTY : new Batch(lines, oldestEventAt);
  }

  // String.length() (UTF-16 units) + newline as a cheap stand-in for UTF-8 bytes: exact for the
  // ASCII JSON , an undercount for non-ASCII
  private static int lineBytes(String line) {
    return line.length() + 1;
  }

  /**
   * One drained, sealed batch. {@code oldestEventAt} is the minimum event time across {@code lines}
   * — queue order is enqueue order, not event-time order, under concurrent publish.
   */
  public record Batch(List<String> lines, Instant oldestEventAt) {
    static final Batch EMPTY = new Batch(List.of(), Instant.EPOCH);

    boolean isEmpty() {
      return lines.isEmpty();
    }

    int size() {
      return lines.size();
    }
  }

  private record Entry(Instant eventAt, String line) {}
}
