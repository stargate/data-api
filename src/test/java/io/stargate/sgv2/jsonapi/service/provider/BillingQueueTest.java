package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
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
