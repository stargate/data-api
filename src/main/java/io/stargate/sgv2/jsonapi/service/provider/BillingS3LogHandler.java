package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.BackPressureStrategy;
import io.smallrye.mutiny.subscription.Cancellable;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.stargate.sgv2.jsonapi.config.BillingS3ExportConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * <p><b>Off the request path.</b> {@link #publish(LogRecord)} only hands the line to an internal
 * pipeline — it never blocks and never throws. The pipeline holds a bounded in-memory backlog
 * ({@link BillingS3ExportConfig#queueCapacity()}) so transient bursts are absorbed and drained as
 * S3 catches up; only when that backlog is full is a line dropped and counted (never silent). The
 * pipeline batches lines and seals a batch on {@link BillingS3ExportConfig#maxEvents()} / {@link
 * BillingS3ExportConfig#maxBytes()} / {@link BillingS3ExportConfig#maxAge()}, then PUTs it with
 * bounded retry/backoff, up to {@link BillingS3ExportConfig#uploadConcurrency()} uploads in flight.
 *
 * <p><b>Verbatim bodies.</b> Each log line is kept byte-for-byte as one NDJSON row — only {@code
 * timestamp} is parsed out (for the key's date path); there is no re-serialization. Each sealed
 * batch is one object at {@code <prefix>/<yyyy>/<MM>/<dd>/<HH>/<mm>/<uuid>.jsonl}; the key is built
 * once and reused across retries so a retried PUT overwrites rather than duplicates (downstream
 * also dedups on each event id).
 *
 * <p>{@link #close()} drains in-flight batches (bounded) and closes the uploader. The handler is
 * intentionally <b>not</b> a CDI bean — the installer wires this instance to the {@code
 * billing.events} category explicitly.
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
  // How long {@link #close()} waits for in-flight batches to drain before cancelling.
  private static final long SHUTDOWN_DRAIN_TIMEOUT_MILLIS = 15_000L;

  // ---- Collaborators ----
  private final AsyncBatchUploader uploader;
  private final BillingMetrics metrics;

  // ---- Tuning (resolved from BillingS3ExportConfig) ----
  private final int maxEvents;
  private final long maxBytes;
  private final long maxAgeNanos;
  private final int maxUploadAttempts;
  private final long retryBaseBackoffMillis;
  private final double retryJitter;
  private final long queueCapacity;
  private final int uploadConcurrency;

  // ---- Reactive pipeline ----
  private volatile MultiEmitter<? super String> emitter;
  private final Cancellable subscription;
  private final CountDownLatch terminated = new CountDownLatch(1);

  // ---- Mutable in-flight state ----
  /**
   * Events accepted by {@link #publish} but not yet delivered or failed — the in-memory backlog
   * depth and the bounded-buffer gate. {@code publish} CAS-checks it against {@link
   * #queueCapacity}; {@link BillingMetrics} exposes it as the {@code billing.s3.queue.depth} gauge.
   */
  private final AtomicLong backlogEvents = new AtomicLong(0);

  /**
   * Current open (unsealed) batch — accumulation state carried across calls. No lock needed: the
   * upstream {@code SerializedMultiEmitter} serializes onItem, so {@link #accumulate} and {@link
   * #flushOpenBatch} never run concurrently.
   */
  private Batch openBatch;

  /** Config-driven constructor used by the installer. */
  public BillingS3LogHandler(
      BillingS3ExportConfig config, AsyncBatchUploader uploader, MeterRegistry meterRegistry) {
    this(
        uploader,
        meterRegistry,
        config.maxEvents(),
        config.maxBytes(),
        config.maxAge(),
        config.maxUploadAttempts(),
        config.retryBaseBackoff(),
        config.retryJitter(),
        config.queueCapacity(),
        config.uploadConcurrency());
  }

  /** Explicit-threshold constructor; convenient for unit tests. */
  BillingS3LogHandler(
      AsyncBatchUploader uploader,
      MeterRegistry meterRegistry,
      int maxEvents,
      long maxBytes,
      Duration maxAge,
      int maxUploadAttempts,
      Duration retryBaseBackoff,
      double retryJitter,
      int queueCapacity,
      int uploadConcurrency) {
    this.uploader = uploader;
    this.maxEvents = Math.max(1, maxEvents);
    this.maxBytes = Math.max(1L, maxBytes);
    this.maxAgeNanos = Math.max(1L, maxAge.toNanos());
    this.maxUploadAttempts = Math.max(1, maxUploadAttempts);
    this.retryBaseBackoffMillis = Math.max(0L, retryBaseBackoff.toMillis());
    this.retryJitter = Math.clamp(retryJitter, 0.0, 1.0);
    this.queueCapacity = Math.max(1L, queueCapacity);
    this.uploadConcurrency = Math.max(1, uploadConcurrency);
    this.metrics = new BillingMetrics(meterRegistry, backlogEvents, this.queueCapacity);

    this.subscription =
        Multi.createFrom()
            .<String>emitter(em -> this.emitter = em, BackPressureStrategy.BUFFER)
            .onItem()
            .transform(this::parse)
            .onItem()
            .transformToMultiAndConcatenate(this::accumulate)
            .onCompletion()
            .switchTo(this::flushOpenBatch)
            .onItem()
            .transformToUni(this::uploadWithRetry)
            .merge(this.uploadConcurrency)
            .onTermination()
            .invoke(() -> terminated.countDown())
            .subscribe()
            .with(
                ignored -> {},
                failure -> LOG.error("Billing S3 export pipeline terminated", failure));
  }

  // ============================================================
  // java.util.logging.Handler
  // ============================================================

  @Override
  public void publish(LogRecord record) {
    MultiEmitter<? super String> e = this.emitter;
    if (record == null) {
      return;
    }
    String line = record.getMessage();
    if (e == null || line == null || line.isEmpty()) {
      return;
    }
    metrics.recordOffered();
    // Bounded backlog: accept while under capacity (absorbing bursts); once full, shed and count —
    // never a silent drop. The CAS keeps the bound exact under concurrent publish().
    long current;
    do {
      current = backlogEvents.get();
      if (current >= queueCapacity) {
        metrics.recordDropped();
        return;
      }
    } while (!backlogEvents.compareAndSet(current, current + 1));
    e.emit(line);
  }

  @Override
  public void flush() {
    // No-op: the pipeline ships continuously; close() handles the final seal-everything on
    // shutdown.
  }

  @Override
  public void close() {
    MultiEmitter<? super String> e = this.emitter;
    if (e != null) {
      e.complete();
    }
    try {
      if (!terminated.await(SHUTDOWN_DRAIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
        LOG.warn(
            "Billing S3 export did not drain within {} ms on shutdown; cancelling",
            SHUTDOWN_DRAIN_TIMEOUT_MILLIS);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    if (subscription != null) {
      subscription.cancel();
    }
    try {
      uploader.close();
    } catch (Exception ex) {
      LOG.warn("Error closing billing S3 uploader", ex);
    }
  }

  // ============================================================
  // Batching
  // ============================================================

  /**
   * Sequential fold of one parsed row into {@link #openBatch}, emitting 0–2 sealed {@link Batch}es
   * (sealed on {@code maxEvents}/{@code maxBytes}, or the prior batch on {@code maxAge} at the next
   * arrival). Kept ordered and one-at-a-time via {@code …AndConcatenate}, not merge.
   */
  private Multi<Batch> accumulate(Parsed parsed) {
    List<Batch> sealed = new ArrayList<>(2);
    Batch batch = openBatch;
    if (batch != null && System.nanoTime() - batch.firstNanos >= maxAgeNanos) {
      sealed.add(batch);
      batch = null;
      openBatch = null;
    }
    if (batch == null) {
      batch = new Batch(parsed.timestamp());
      openBatch = batch;
    }
    batch.add(parsed.line());
    if (batch.events >= maxEvents || batch.bytes >= maxBytes) {
      sealed.add(batch);
      openBatch = null;
    }
    return Multi.createFrom().iterable(sealed);
  }

  /** Emits the final open batch (if any) when the stream completes. */
  private Multi<Batch> flushOpenBatch() {
    Batch remaining = openBatch;
    openBatch = null;
    return remaining == null ? Multi.createFrom().empty() : Multi.createFrom().item(remaining);
  }

  /**
   * Uploads one sealed batch with bounded retry/backoff/jitter. Never propagates failure: a
   * giving-up batch is counted and recovered to a no-op so the pipeline stays alive.
   */
  private Uni<Void> uploadWithRetry(Batch batch) {
    String key = objectKey(batch.firstTimestamp, UUID.randomUUID());
    byte[] body = batch.body();

    Uni<Void> put = Uni.createFrom().completionStage(() -> uploader.upload(key, body));
    if (maxUploadAttempts > 1) {
      var retry = put.onFailure().retry();
      put =
          retryBaseBackoffMillis > 0
              ? retry
                  .withBackOff(Duration.ofMillis(retryBaseBackoffMillis))
                  .withJitter(retryJitter)
                  .atMost(maxUploadAttempts - 1)
              : retry.atMost(maxUploadAttempts - 1);
    }

    return put.onItem()
        .invoke(
            () -> {
              metrics.recordBatchDelivered(batch.events);
              backlogEvents.addAndGet(-batch.events);
            })
        .onFailure()
        .invoke(
            t -> LOG.error("Giving up on billing S3 batch '{}' ({} events)", key, batch.events, t))
        .onFailure()
        .recoverWithItem(
            () -> {
              metrics.recordBatchFailed(batch.events);
              backlogEvents.addAndGet(-batch.events);
              return null;
            });
  }

  /** Object key: {@code <prefix>/<yyyy>/<MM>/<dd>/<HH>/<mm>/<uuid>.jsonl} (UTC). */
  static String objectKey(Instant timestamp, UUID id) {
    return PATH_PREFIX + "/" + KEY_TIME_FORMAT.format(timestamp) + "/" + id + ".jsonl";
  }

  /** Parses the {@code timestamp} for the key's date path; keeps the verbatim line. */
  private Parsed parse(String line) {
    try {
      JsonNode node = MAPPER.readTree(line);
      JsonNode tsNode = node.get("timestamp");
      if (tsNode != null && tsNode.isTextual()) {
        return new Parsed(line, Instant.parse(tsNode.asText()));
      }
    } catch (Exception e) {
      // fall through to the wall-clock fallback below
    }
    metrics.recordParseFailure();
    return new Parsed(line, Instant.now());
  }

  private record Parsed(String line, Instant timestamp) {}

  /** A growing set of verbatim NDJSON rows. */
  private static final class Batch {
    private final Instant firstTimestamp;
    private final long firstNanos = System.nanoTime();
    private final List<String> lines = new ArrayList<>();
    private int events = 0;
    private long bytes = 0;

    Batch(Instant firstTimestamp) {
      this.firstTimestamp = firstTimestamp;
    }

    void add(String line) {
      lines.add(line);
      events++;
      bytes += line.getBytes(StandardCharsets.UTF_8).length + 1L; // +1 for the newline
    }

    byte[] body() {
      StringBuilder sb = new StringBuilder((int) Math.min(Integer.MAX_VALUE, bytes + events));
      for (String line : lines) {
        sb.append(line).append('\n');
      }
      return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
  }

  /**
   * Single-attempt async uploader of one sealed batch (test seam); the production implementation is
   * {@link S3BatchUploader}. A failed {@link CompletionStage} triggers the handler's retry/backoff.
   */
  @FunctionalInterface
  public interface AsyncBatchUploader extends AutoCloseable {
    CompletionStage<Void> upload(String key, byte[] body);

    @Override
    default void close() {}
  }
}
