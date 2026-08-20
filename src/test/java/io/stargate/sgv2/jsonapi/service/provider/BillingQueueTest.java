package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link BillingQueue}: seal thresholds, drain limits, and batch metadata. */
class BillingQueueTest {

  private static final Instant T0 = Instant.parse("2026-05-20T14:23:11Z");

  @Test
  void sealsByEventCount() {
    var queue = new BillingQueue(2, 1_000_000, 10);

    queue.offer(T0, "a");
    assertThat(queue.shouldFlush()).isFalse();
    queue.offer(T0, "b");
    assertThat(queue.shouldFlush()).isTrue();
  }

  @Test
  void sealsByBufferedBytes() {
    // Each line counts as length + 1 (newline): "aaaa" = 5 bytes.
    var queue = new BillingQueue(100, 10, 10);

    queue.offer(T0, "aaaa");
    assertThat(queue.shouldFlush()).isFalse();
    queue.offer(T0, "bbbb");
    assertThat(queue.shouldFlush()).isTrue();
  }

  @Test
  void drainStopsAtMaxEventsAndLeavesTheRemainder() {
    var queue = new BillingQueue(2, 1_000_000, 10);
    queue.offer(T0, "a");
    queue.offer(T0, "b");
    queue.offer(T0, "c");

    assertThat(queue.drain().lines()).containsExactly("a", "b");
    assertThat(queue.drain().lines()).containsExactly("c");
    assertThat(queue.drain().isEmpty()).isTrue();
  }

  @Test
  void drainStopsAtMaxBytesAndLeavesTheRemainder() {
    var queue = new BillingQueue(100, 10, 10);
    queue.offer(T0, "aaaa");
    queue.offer(T0, "bbbb");
    queue.offer(T0, "cccc");

    assertThat(queue.drain().lines()).containsExactly("aaaa", "bbbb");
    assertThat(queue.drain().lines()).containsExactly("cccc");
    assertThat(queue.drain().isEmpty()).isTrue();
  }

  @Test
  void oldestEventAtIsTheMinimumAcrossTheBatchNotTheHead() {
    var queue = new BillingQueue(10, 1_000_000, 10);
    // Concurrent publishes can enqueue out of event-time order; the head is not the oldest.
    queue.offer(T0.plusSeconds(5), "enqueued-first-but-newer");
    queue.offer(T0, "enqueued-second-but-older");

    assertThat(queue.drain().oldestEventAt()).isEqualTo(T0);
  }

  @Test
  void offerRejectsWhenFull() {
    var queue = new BillingQueue(10, 1_000_000, 2);

    assertThat(queue.offer(T0, "a")).isTrue();
    assertThat(queue.offer(T0, "b")).isTrue();
    assertThat(queue.offer(T0, "c")).isFalse();
    assertThat(queue.size()).isEqualTo(2);
  }

  @Test
  void concurrentOfferAndDrainKeepsAccountingConsistent() throws Exception {
    var queue = new BillingQueue(10, 1_000_000, 5_000);
    int threads = 4;
    int perThread = 500;
    Set<String> published = ConcurrentHashMap.newKeySet();
    List<String> drained = new ArrayList<>(); // touched only by the drainer thread until join

    AtomicBoolean producersDone = new AtomicBoolean(false);
    Thread drainer =
        new Thread(
            () -> {
              while (!producersDone.get() || !queue.isEmpty()) {
                var batch = queue.drain();
                if (batch.isEmpty()) {
                  Thread.onSpinWait();
                } else {
                  drained.addAll(batch.lines());
                }
              }
            },
            "billing-queue-test-drainer");
    drainer.start();

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
                  for (int i = 0; i < perThread; i++) {
                    String line = "{\"t\":" + threadId + ",\"i\":" + i + "}";
                    published.add(line);
                    assertThat(queue.offer(T0, line)).isTrue(); // capacity is never reached
                  }
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
    producersDone.set(true);
    drainer.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(drainer.isAlive()).isFalse();

    // Every offered line is drained exactly once, and the byte accounting lands back on zero:
    // concurrent add/subtract may transiently disagree, but the settled state must not drift.
    assertThat(drained).hasSize(threads * perThread);
    assertThat(new HashSet<>(drained)).isEqualTo(published);
    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.queuedBytes()).isZero();
  }

  @Test
  void byteSealResetsOnceDrained() {
    var queue = new BillingQueue(100, 10, 10);
    queue.offer(T0, "aaaa");
    queue.offer(T0, "bbbb");
    assertThat(queue.shouldFlush()).isTrue();

    queue.drain();

    assertThat(queue.isEmpty()).isTrue();
    assertThat(queue.shouldFlush()).isFalse(); // queuedBytes went back down with the drain
  }
}
