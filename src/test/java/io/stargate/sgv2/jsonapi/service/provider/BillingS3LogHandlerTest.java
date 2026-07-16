package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BillingS3LogHandler}: the flush triggers (seal on publish, age tick, drain
 * on close), the upload-concurrency gate, failure containment, and the at-most-once accounting
 * invariants under concurrent publish. The uploader is a programmable in-memory fake; real S3 I/O
 * is covered by {@code BillingS3ExportIntegrationTest}.
 *
 * <p>Ordering is only asserted in single-producer tests (the buffer is FIFO for a single thread);
 * concurrency tests assert set-equality and counter reconciliation, never interleaving order.
 */
class BillingS3LogHandlerTest {

  private static final Duration AWAIT = Duration.ofSeconds(10);
  private static final Duration NEVER = Duration.ofHours(1);
  private static final Instant T0 = Instant.parse("2026-05-20T14:23:11Z");

  /**
   * First-ever Uni creation in a JVM registers the SmallRye context-propagation provider through
   * {@code ContextManagerProvider.instance()}, whose ServiceLoader loop both CAS-races concurrent
   * callers and throws "ContextManagerProvider already set" when it discovers a second provider —
   * possibly after having registered the first. Racing that from concurrent producer threads makes
   * publish() throw. Quarkus registers the provider single-threaded at boot, so only this
   * bare-JUnit JVM needs the deterministic warm-up.
   */
  @BeforeAll
  static void warmUpMutinyInfrastructure() {
    try {
      io.smallrye.context.SmallRyeContextManagerProvider.getManager();
    } catch (IllegalStateException alreadySetOrDuplicate) {
      // The provider is registered even when the duplicate-discovery branch throws; either way
      // ContextManagerProvider.INSTANCE is now set and concurrent callers can no longer race it.
    }
    Uni.createFrom()
        .item(0)
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .await()
        .atMost(AWAIT);
  }

  private static final String OFFERED = "billing.s3.events.offered";
  private static final String FLUSHED = "billing.s3.events.flushed";
  private static final String EVENTS_FAILED = "billing.s3.events.failed";
  private static final String BATCHES_UPLOADED = "billing.s3.batches.uploaded";
  private static final String BATCHES_FAILED = "billing.s3.batches.failed";
  private static final String DROPPED = "billing.s3.events.dropped";

  // ============================================================
  // Fake uploader
  // ============================================================

  /**
   * Programmable {@link BillingS3LogHandler.AsyncBatchUploader}: records every batch and settles
   * the returned Uni per {@link Mode}. Never blocks a caller thread — HOLD parks the completion in
   * {@code held} for the test to release explicitly.
   */
  static final class RecordingUploader implements BillingS3LogHandler.AsyncBatchUploader {
    enum Mode {
      COMPLETE,
      HOLD,
      FAIL,
      THROW_SYNC
    }

    volatile Mode mode = Mode.COMPLETE;
    final List<BillingQueue.Batch> batches = new CopyOnWriteArrayList<>();
    final BlockingQueue<CompletableFuture<Void>> held = new LinkedBlockingQueue<>();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    volatile boolean closed;

    @Override
    public Uni<Void> upload(BillingQueue.Batch batch) {
      batches.add(batch);
      if (mode == Mode.THROW_SYNC) {
        throw new RuntimeException("simulated synchronous uploader failure");
      }
      int now = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(now, Math::max);
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.whenComplete((v, t) -> inFlight.decrementAndGet());
      switch (mode) {
        case COMPLETE -> future.complete(null);
        case FAIL -> future.completeExceptionally(new RuntimeException("simulated upload failure"));
        case HOLD -> held.add(future);
        default -> throw new IllegalStateException("unexpected mode " + mode);
      }
      return Uni.createFrom().completionStage(future);
    }

    /** Completes one held upload, waiting for it to exist first. */
    void releaseOne() throws InterruptedException {
      CompletableFuture<Void> future = held.poll(AWAIT.toSeconds(), TimeUnit.SECONDS);
      assertThat(future).as("a held upload to release").isNotNull();
      future.complete(null);
    }

    /** Switches to pass-through and completes everything currently held. */
    void releaseAllAndComplete() {
      mode = Mode.COMPLETE;
      CompletableFuture<Void> future;
      while ((future = held.poll()) != null) {
        future.complete(null);
      }
    }

    List<String> allLines() {
      return batches.stream().flatMap(b -> b.lines().stream()).toList();
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  // ============================================================
  // Helpers
  // ============================================================

  private static BillingS3LogHandler newHandler(
      RecordingUploader uploader,
      SimpleMeterRegistry registry,
      int maxEvents,
      long maxBytes,
      int queueCapacity,
      int uploadConcurrency) {
    return newHandler(
        uploader,
        registry,
        maxEvents,
        maxBytes,
        queueCapacity,
        uploadConcurrency,
        Duration.ofSeconds(5));
  }

  private static BillingS3LogHandler newHandler(
      RecordingUploader uploader,
      SimpleMeterRegistry registry,
      int maxEvents,
      long maxBytes,
      int queueCapacity,
      int uploadConcurrency,
      Duration shutdownTimeout) {
    return new BillingS3LogHandler(
        uploader,
        registry,
        maxEvents,
        maxBytes,
        NEVER,
        queueCapacity,
        uploadConcurrency,
        shutdownTimeout);
  }

  private static LogRecord record(String message) {
    return record(T0, message);
  }

  private static LogRecord record(Instant at, String message) {
    LogRecord logRecord = new LogRecord(Level.INFO, message);
    logRecord.setInstant(at);
    return logRecord;
  }

  private static double counter(SimpleMeterRegistry registry, String name, String... tags) {
    return registry.counter(name, tags).count();
  }

  // ============================================================
  // Behavior — publish and flush triggers
  // ============================================================

  @Test
  void publishIgnoresNullRecordAndBlankLines() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 1, 1_000_000, 10, 1);
    try {
      handler.publish(null);
      handler.publish(record(null));
      handler.publish(record("   "));

      assertThat(uploader.batches).isEmpty();
      assertThat(counter(registry, OFFERED)).isZero();
    } finally {
      handler.close();
    }
  }

  @Test
  void sealsByCountAndShipsExactBatch() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 3, 1_000_000, 10, 2);
    try {
      // Enqueue order is publish order for a single producer; event-time order is not (the second
      // record is older on purpose, so oldestEventAt must be the min, not the head).
      handler.publish(record(T0.plusSeconds(5), "{\"e\":1}"));
      handler.publish(record(T0, "{\"e\":2}"));
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .until(() -> uploader.batches.isEmpty());

      handler.publish(record(T0.plusSeconds(9), "{\"e\":3}"));

      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));
      var batch = uploader.batches.get(0);
      assertThat(batch.lines()).containsExactly("{\"e\":1}", "{\"e\":2}", "{\"e\":3}");
      assertThat(batch.oldestEventAt()).isEqualTo(T0);
      await()
          .atMost(AWAIT)
          .untilAsserted(
              () -> {
                assertThat(counter(registry, OFFERED)).isEqualTo(3.0);
                assertThat(counter(registry, FLUSHED)).isEqualTo(3.0);
                assertThat(counter(registry, BATCHES_UPLOADED)).isEqualTo(1.0);
                assertThat(counter(registry, DROPPED, "reason", "capacity")).isZero();
              });
    } finally {
      handler.close();
    }
  }

  @Test
  void sealsByBufferedBytes() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    // Lines count as length + 1: two 4-char lines hit the 10-byte seal together.
    var handler = newHandler(uploader, registry, 100, 10, 10, 2);
    try {
      handler.publish(record("aaaa"));
      handler.publish(record("bbbb"));

      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));
      assertThat(uploader.batches.get(0).lines()).containsExactly("aaaa", "bbbb");
    } finally {
      handler.close();
    }
  }

  @Test
  void noShipmentBelowSealUntilAgeTick() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 100, 1_000_000, 10, 2);
    try {
      handler.publish(record("{\"e\":1}"));
      handler.publish(record("{\"e\":2}"));
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .until(() -> uploader.batches.isEmpty());

      // Deterministic age trigger: call the tick directly instead of waiting for the scheduler.
      handler.onAgeTick();

      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));
      assertThat(uploader.batches.get(0).lines()).containsExactly("{\"e\":1}", "{\"e\":2}");
    } finally {
      handler.close();
    }
  }

  @Test
  void ageTickIsScheduledForReal() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler =
        new BillingS3LogHandler(
            uploader,
            registry,
            100,
            1_000_000,
            Duration.ofMillis(100),
            10,
            2,
            Duration.ofSeconds(5));
    try {
      handler.publish(record("{\"e\":1}"));

      // No seal is reached; only the scheduled fixed-rate tick can ship this line.
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).isNotEmpty());
      assertThat(uploader.allLines()).containsExactly("{\"e\":1}");
    } finally {
      handler.close();
    }
  }

  // ============================================================
  // Behavior — failure containment
  // ============================================================

  @Test
  void uploadFailureCountsBatchAndPipelineSurvives() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 2, 1_000_000, 10, 1);
    try {
      uploader.mode = RecordingUploader.Mode.FAIL;
      handler.publish(record("{\"e\":1}"));
      handler.publish(record("{\"e\":2}"));

      await()
          .atMost(AWAIT)
          .untilAsserted(
              () -> {
                assertThat(counter(registry, BATCHES_FAILED)).isEqualTo(1.0);
                assertThat(counter(registry, EVENTS_FAILED)).isEqualTo(2.0);
              });

      // The failure released the in-flight slot: the next sealed batch still ships.
      uploader.mode = RecordingUploader.Mode.COMPLETE;
      handler.publish(record("{\"e\":3}"));
      handler.publish(record("{\"e\":4}"));

      await()
          .atMost(AWAIT)
          .untilAsserted(
              () -> {
                assertThat(counter(registry, FLUSHED)).isEqualTo(2.0);
                assertThat(counter(registry, BATCHES_UPLOADED)).isEqualTo(1.0);
              });
    } finally {
      handler.close();
    }
  }

  @Test
  void synchronousUploaderThrowIsContained() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 2, 1_000_000, 10, 1);
    try {
      uploader.mode = RecordingUploader.Mode.THROW_SYNC;
      handler.publish(record("{\"e\":1}"));
      handler.publish(record("{\"e\":2}"));

      await()
          .atMost(AWAIT)
          .untilAsserted(
              () -> {
                assertThat(counter(registry, BATCHES_FAILED)).isEqualTo(1.0);
                assertThat(counter(registry, EVENTS_FAILED)).isEqualTo(2.0);
              });

      uploader.mode = RecordingUploader.Mode.COMPLETE;
      handler.publish(record("{\"e\":3}"));
      handler.publish(record("{\"e\":4}"));

      await()
          .atMost(AWAIT)
          .untilAsserted(() -> assertThat(counter(registry, FLUSHED)).isEqualTo(2.0));
    } finally {
      handler.close();
    }
  }

  // ============================================================
  // Behavior — close
  // ============================================================

  @Test
  void closeDrainsRemainderAndClosesUploader() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 100, 1_000_000, 10, 2);

    handler.publish(record("{\"e\":1}"));
    handler.publish(record("{\"e\":2}"));
    handler.publish(record("{\"e\":3}"));
    handler.close();

    // close() is synchronous: by the time it returns the drain has settled and counted.
    assertThat(uploader.allLines()).containsExactly("{\"e\":1}", "{\"e\":2}", "{\"e\":3}");
    assertThat(uploader.closed).isTrue();
    assertThat(counter(registry, FLUSHED)).isEqualTo(3.0);
    assertThat(counter(registry, DROPPED, "reason", "shutdown")).isZero();
  }

  @Test
  void closeTimeoutCountsAbandoned() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 1, 1_000_000, 10, 1, Duration.ofMillis(200));
    try {
      uploader.mode = RecordingUploader.Mode.HOLD;
      handler.publish(record("{\"e\":1}"));
      // Wait until the first batch is in flight (and held) so the queued remainder is exact.
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));
      handler.publish(record("{\"e\":2}"));
      handler.publish(record("{\"e\":3}"));

      long startNanos = System.nanoTime();
      handler.close();
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

      assertThat(elapsed).isLessThan(Duration.ofSeconds(3));
      assertThat(counter(registry, DROPPED, "reason", "shutdown")).isEqualTo(2.0);
      assertThat(uploader.closed).isTrue();
    } finally {
      uploader.releaseAllAndComplete();
    }
  }

  @Test
  void closeIsIdempotentAndPublishAfterCloseIsSafe() {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 1, 1_000_000, 10, 1);

    handler.close();
    handler.close(); // JUL Handler.close() contract: idempotent

    // A racing thread may publish after close; it must never throw (JUL handler contract).
    handler.publish(record("{\"late\":1}"));
    assertThat(counter(registry, OFFERED)).isEqualTo(1.0);
  }

  // ============================================================
  // Concurrency — invariant style, no interleaving assertions
  // ============================================================

  @Test
  void multiProducerNoLossNoDuplication() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 50, 1_000_000_000L, 10_000, 4);

    int threads = 8;
    int perThread = 500;
    Set<String> published = ConcurrentHashMap.newKeySet();
    runProducers(
        threads,
        (threadId) -> {
          for (int i = 0; i < perThread; i++) {
            String line = "{\"t\":" + threadId + ",\"i\":" + i + "}";
            published.add(line);
            handler.publish(record(line));
          }
        });
    handler.close();

    List<String> delivered = uploader.allLines();
    assertThat(delivered).hasSize(threads * perThread);
    assertThat(new HashSet<>(delivered)).isEqualTo(published);
    assertThat(counter(registry, OFFERED)).isEqualTo(threads * perThread);
    assertThat(counter(registry, FLUSHED)).isEqualTo(threads * perThread);
    assertThat(counter(registry, DROPPED, "reason", "capacity")).isZero();
    assertThat(counter(registry, DROPPED, "reason", "shutdown")).isZero();
  }

  @Test
  void overflowAccountingReconciles() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    // No seal is ever reached (count seal above capacity, byte seal huge): nothing drains while
    // producers run, so every line beyond the 64-slot buffer is a deterministic capacity drop.
    var handler = newHandler(uploader, registry, 1000, 1_000_000_000L, 64, 4);

    int threads = 4;
    int perThread = 500;
    Set<String> published = ConcurrentHashMap.newKeySet();
    runProducers(
        threads,
        (threadId) -> {
          for (int i = 0; i < perThread; i++) {
            String line = "{\"t\":" + threadId + ",\"i\":" + i + "}";
            published.add(line);
            handler.publish(record(line));
          }
        });

    uploader.releaseAllAndComplete();
    handler.close();

    double offered = counter(registry, OFFERED);
    double flushed = counter(registry, FLUSHED);
    double droppedCapacity = counter(registry, DROPPED, "reason", "capacity");
    double droppedShutdown = counter(registry, DROPPED, "reason", "shutdown");
    assertThat(offered).isEqualTo(threads * perThread);
    assertThat(flushed).isEqualTo(64.0);
    assertThat(droppedCapacity).isEqualTo(threads * perThread - 64.0);
    assertThat(flushed + droppedCapacity + droppedShutdown).isEqualTo(offered);

    List<String> delivered = uploader.allLines();
    assertThat(delivered).doesNotHaveDuplicates();
    assertThat(published).containsAll(delivered);
  }

  @Test
  void publishNeverBlocksWhenUploaderStalls() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 1, 1_000_000, 8, 1);
    try {
      uploader.mode = RecordingUploader.Mode.HOLD;
      handler.publish(record("{\"i\":0}"));
      // Wait for the single slot to be claimed and its 1-line batch drained: from here the queue
      // is empty, the slot is stuck, and every subsequent count is deterministic.
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));

      long startNanos = System.nanoTime();
      for (int i = 1; i < 50; i++) {
        handler.publish(record("{\"i\":" + i + "}"));
      }
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

      assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
      assertThat(counter(registry, OFFERED)).isEqualTo(50.0);
      // 1 in flight + 8 buffered; the other 41 dropped without ever blocking the caller.
      assertThat(counter(registry, DROPPED, "reason", "capacity")).isEqualTo(41.0);
    } finally {
      uploader.releaseAllAndComplete();
      handler.close();
    }
  }

  @Test
  void concurrencyGateCapsParallelUploads() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 1, 1_000_000, 100, 2);
    try {
      uploader.mode = RecordingUploader.Mode.HOLD;
      for (int i = 0; i < 10; i++) {
        handler.publish(record("{\"i\":" + i + "}"));
      }

      // Both slots claim work; the rest stays queued behind the gate.
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.inFlight.get()).isEqualTo(2));

      // Each release lets exactly the next batch through, one at a time.
      while (uploader.batches.size() < 10) {
        uploader.releaseOne();
        int expected = Math.min(uploader.batches.size() + 1, 10);
        await()
            .atMost(AWAIT)
            .untilAsserted(
                () -> assertThat(uploader.batches.size()).isGreaterThanOrEqualTo(expected));
      }

      assertThat(uploader.maxInFlight.get()).isEqualTo(2);
      assertThat(uploader.allLines()).hasSize(10).doesNotHaveDuplicates();
    } finally {
      uploader.releaseAllAndComplete();
      handler.close();
    }
  }

  @Test
  void settledUploadChainsNextBatchWithoutNewPublish() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 2, 1_000_000, 100, 1);
    try {
      uploader.mode = RecordingUploader.Mode.HOLD;
      for (int i = 1; i <= 6; i++) {
        handler.publish(record("{\"i\":" + i + "}"));
      }

      // Single slot: exactly one upload starts, the two other sealed batches wait behind it.
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(1));
      await()
          .during(Duration.ofMillis(200))
          .atMost(Duration.ofSeconds(2))
          .until(() -> uploader.batches.size() == 1);
      assertThat(uploader.batches.get(0).lines()).containsExactly("{\"i\":1}", "{\"i\":2}");

      // No further publish happens: each settle must chain the next flush on its own.
      uploader.releaseOne();
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(2));
      assertThat(uploader.batches.get(1).lines()).containsExactly("{\"i\":3}", "{\"i\":4}");

      uploader.releaseOne();
      await().atMost(AWAIT).untilAsserted(() -> assertThat(uploader.batches).hasSize(3));
      assertThat(uploader.batches.get(2).lines()).containsExactly("{\"i\":5}", "{\"i\":6}");

      uploader.releaseOne();
    } finally {
      uploader.releaseAllAndComplete();
      handler.close();
    }
  }

  @Test
  void closeRacingProducersNeverHangsAndReconciles() throws Exception {
    var uploader = new RecordingUploader();
    var registry = new SimpleMeterRegistry();
    var handler = newHandler(uploader, registry, 5, 1_000_000_000L, 1000, 4, Duration.ofSeconds(1));

    int threads = 4;
    // Producers run flat out until stopped; the cap only bounds memory and assertion cost, sized
    // so publishing is still in full flight when close() lands ~50ms in.
    int perThreadCap = 200_000;
    Set<String> published = ConcurrentHashMap.newKeySet();
    List<Throwable> producerErrors = new CopyOnWriteArrayList<>();
    AtomicBoolean stop = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      int threadId = t;
      futures.add(
          executor.submit(
              () -> {
                for (int i = 0; i < perThreadCap && !stop.get(); i++) {
                  String line = "{\"t\":" + threadId + ",\"i\":" + i + "}";
                  published.add(line);
                  try {
                    handler.publish(record(line));
                  } catch (Throwable error) {
                    producerErrors.add(error);
                    return;
                  }
                }
              }));
    }

    Thread.sleep(50); // let producers overlap the close below
    handler.close();
    stop.set(true);
    for (Future<?> future : futures) {
      future.get(AWAIT.toSeconds(), TimeUnit.SECONDS);
    }
    executor.shutdown();

    // publish must never throw, close must return, and the books must stay consistent — lines
    // published after close may be dropped without being counted, so the reconciliation is <=.
    // Plain java.util.Set operations keep these checks O(n); AssertJ's containsAll would scan.
    assertThat(producerErrors).isEmpty();
    List<String> delivered = uploader.allLines();
    Set<String> deliveredSet = new HashSet<>(delivered);
    assertThat(deliveredSet).as("delivered lines must not repeat").hasSize(delivered.size());
    assertThat(published.containsAll(deliveredSet))
        .as("every delivered line must have been published")
        .isTrue();
    double flushed = counter(registry, FLUSHED);
    double droppedCapacity = counter(registry, DROPPED, "reason", "capacity");
    double droppedShutdown = counter(registry, DROPPED, "reason", "shutdown");
    assertThat(flushed + droppedCapacity + droppedShutdown)
        .isLessThanOrEqualTo(counter(registry, OFFERED));
  }

  // ============================================================
  // Producer harness
  // ============================================================

  private interface Producer {
    void run(int threadId) throws Exception;
  }

  /** Runs one producer per thread, released simultaneously, and rethrows any producer failure. */
  private static void runProducers(int threads, Producer producer) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        int threadId = t;
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  producer.run(threadId);
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdown();
    }
  }
}
