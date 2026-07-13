package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Handler} that ships {@code billing.events} JSON log lines to S3 as NDJSON ({@code
 * .jsonl}) objects. Installed on the {@code billing.events} logger by {@link
 * BillingS3HandlerInstaller} when {@link BillingS3ExportConfig#enabled()} is {@code true}; the
 * existing console handler stays attached as a backstop (dual-write).
 *
 * <p><b>Off the request path.</b> {@link #publish(LogRecord)} only offers the line to a bounded
 * in-memory queue — it never blocks and never throws. When the queue holds a full batch ({@link
 * BillingS3ExportConfig#maxEvents()} lines) a flush is dispatched to a worker-pool thread: it
 * drains up to that many lines and PUTs them as one object. Up to {@link
 * BillingS3ExportConfig#uploadConcurrency()} flushes run at once — a non-blocking in-flight counter
 * is the gate, and the S3 PUT is async so worker threads are not held during upload. When the queue
 * is full, further lines are dropped and counted (never silent).
 *
 * <p><b>Verbatim bodies.</b> Each log line is kept byte-for-byte as one NDJSON row; only the
 * batch's first line is parsed (for the key's date path). Each flushed batch is one object at
 * {@code <prefix>/<yyyy>/<MM>/<dd>/<HH>/<mm>/<uuid>.jsonl}; the key is built once and reused across
 * the uploader's retries so a retried PUT overwrites rather than duplicates.
 *
 * <p>{@link #close()} drains whatever remains (including a final under-batch tail), waits for
 * in-flight flushes to settle, then closes the uploader. The handler is intentionally <b>not</b> a
 * CDI bean — the installer wires this instance to the {@code billing.events} category explicitly.
 */
public final class BillingS3LogHandler extends Handler {

  // ---- Constants ----
  // S3 object-key consistent identifier; TBD
  static final String PATH_PREFIX = "billing-events";
  private static final Logger LOG = LoggerFactory.getLogger(BillingS3LogHandler.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // UTC, minute-resolution date path for the object key
  private static final DateTimeFormatter KEY_TIME_FORMAT =
      DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm").withZone(ZoneOffset.UTC);
  // How long {@link #close()} waits for the remaining lines and in-flight flushes to drain.
  private static final long SHUTDOWN_DRAIN_TIMEOUT_MILLIS = 15_000L;

  // ---- Collaborators ----
  private final AsyncBatchUploader uploader;
  private final BillingMetrics metrics;

  // ---- Tuning (resolved from BillingS3ExportConfig) ----
  private final int batchSize; // flush trigger + max lines per object (config.maxEvents)
  private final int uploadConcurrency; // max flushes (S3 PUTs) in flight at once

  // ---- Queue + concurrency gate ----
  private final BlockingQueue<String> queue;

  /**
   * Flushes (S3 PUTs) currently in flight — the non-blocking concurrency gate, capped at {@link
   * #uploadConcurrency}. A slot is held from {@link #drain} through the PUT's completion.
   */
  private final AtomicInteger inFlight = new AtomicInteger(0);

  /** Config-driven constructor used by the installer. */
  public BillingS3LogHandler(
      BillingS3ExportConfig config, AsyncBatchUploader uploader, MeterRegistry meterRegistry) {
    this(
        uploader,
        meterRegistry,
        config.maxEvents(),
        config.maxBytes(),
        config.maxAge(),
        config.queueCapacity(),
        config.uploadConcurrency());
  }

  /**
   * Explicit-threshold constructor; convenient for unit tests. {@code maxBytes} and {@code maxAge}
   * are accepted for config compatibility but not yet wired (object byte-cap and time-based flush
   * are a follow-up); flushing is currently count-based on {@code maxEvents}.
   */
  @VisibleForTesting
  BillingS3LogHandler(
      AsyncBatchUploader uploader,
      MeterRegistry meterRegistry,
      int maxEvents,
      long maxBytes,
      Duration maxAge,
      int queueCapacity,
      int uploadConcurrency) {
    this.uploader = uploader;
    this.batchSize = maxEvents;
    this.uploadConcurrency = uploadConcurrency;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.metrics = new BillingMetrics(meterRegistry, queue::size, queueCapacity);
  }

  // ============================================================
  // java.util.logging.Handler
  // ============================================================

  @Override
  public void publish(LogRecord record) {
    if (record == null) {
      return;
    }
    String line = record.getMessage();
    if (line == null || line.isBlank()) {
      return;
    }
    metrics.recordOffered();
    if (!queue.offer(line)) {
      // Bounded queue full: shed and count — never block, never throw, never silent.
      metrics.recordDropped();
      return;
    }
    maybeFlush();
  }

  @Override
  public void flush() {
    // No-op: flushes are size-triggered and continuous; close() seals whatever remains on shutdown.
  }

  /**
   * Shutdown drain: serially flushes the buffered backlog (incl. the sub-batch tail the size-gate
   * skips), then waits for in-flight PUTs to finish before closing the client. No internal timeout
   * — bounded by the platform's shutdown grace (SIGKILL).
   */
  @Override
  public void close() {
    List<String> batch;
    while (!(batch = drain()).isEmpty()) {
      uploadBatch(batch).await().indefinitely();
    }
    while (inFlight.get() > 0) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }
    uploader.close();
  }

  // ============================================================
  // Flush pipeline
  // ============================================================

  /**
   * Dispatches one flush if the queue holds a full batch and a concurrency slot is free. Called
   * after every {@link #publish} and again when a flush completes, so the pipeline self-clocks up
   * to {@link #uploadConcurrency} concurrent uploads with no standing reader thread.
   */
  private void maybeFlush() {
    if (queue.size() >= batchSize && tryFlush()) {
      Uni.createFrom()
          .item(this::drain)
          .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
          .flatMap(this::uploadBatch)
          .eventually(
              () -> {
                inFlight.getAndDecrement();
                maybeFlush(); // a full batch may have accumulated while this one uploaded
              })
          .subscribe()
          .with(ignored -> {}, failure -> LOG.error("Billing S3 export flush failed", failure));
    }
  }

  /**
   * Non-blocking CAS gate: claims an in-flight slot iff fewer than {@link #uploadConcurrency} are
   * held.
   */
  private boolean tryFlush() {
    int prev = inFlight.getAndUpdate(n -> n < uploadConcurrency ? n + 1 : n);
    return prev < uploadConcurrency;
  }

  /**
   * Removes up to {@link #batchSize} lines from the queue (may come back empty if another flush
   * raced ahead and took them first).
   */
  private List<String> drain() {
    List<String> batch = new ArrayList<>(batchSize);
    queue.drainTo(batch, batchSize);
    return batch;
  }

  /**
   * Ships one drained batch: the uploader PUTs it as a single NDJSON object, applying its own
   * bounded retry/backoff. Never propagates failure — a giving-up batch is counted and recovered to
   * a no-op so the flush loop stays alive.
   */
  private Uni<Void> uploadBatch(List<String> batch) {
    if (batch.isEmpty()) {
      return Uni.createFrom().voidItem();
    }
    int events = batch.size();
    String key = objectKey(firstTimestamp(batch), UUID.randomUUID());
    byte[] body = toNdjson(batch);
    return uploader
        .upload(key, body)
        .onItem()
        .invoke(() -> metrics.recordBatchDelivered(events))
        .onFailure()
        .invoke(
            t -> LOG.error("Failed to upload billing S3 batch '{}' ({} events)", key, events, t))
        .onFailure()
        .recoverWithItem(
            () -> {
              metrics.recordBatchFailed(events);
              return null;
            });
  }

  /** Object key: {@code <prefix>/<yyyy>/<MM>/<dd>/<HH>/<mm>/<uuid>.jsonl} (UTC). */
  static String objectKey(Instant timestamp, UUID id) {
    return PATH_PREFIX + "/" + KEY_TIME_FORMAT.format(timestamp) + "/" + id + ".jsonl";
  }

  /** NDJSON body: each line verbatim, newline-terminated. */
  private static byte[] toNdjson(List<String> batch) {
    StringBuilder sb = new StringBuilder();
    for (String line : batch) {
      sb.append(line).append('\n');
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Timestamp for the object key's date path — parsed from the batch's first line's {@code
   * timestamp}, falling back to wall clock (and counting the parse failure) when it is
   * absent/unparseable.
   */
  private Instant firstTimestamp(List<String> batch) {
    try {
      JsonNode node = MAPPER.readTree(batch.get(0));
      JsonNode tsNode = node.get("timestamp");
      if (tsNode != null && tsNode.isTextual()) {
        return Instant.parse(tsNode.asText());
      }
    } catch (Exception e) {
      // fall through to the wall-clock fallback below
    }
    metrics.recordParseFailure();
    return Instant.now();
  }

  /**
   * Uploads one drained batch to S3 with its own bounded retry/backoff, failing only once retries
   * are exhausted (see implementation {@link S3BatchUploader}).
   */
  @FunctionalInterface
  public interface AsyncBatchUploader extends AutoCloseable {
    Uni<Void> upload(String key, byte[] body);

    @Override
    default void close() {}
  }
}
