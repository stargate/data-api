package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.metrics.MetricsConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RerankingConcurrencyGate}, the per provider+model bulkhead that bounds in-flight
 * reranking calls per pod and parks the overflow in a bounded FIFO queue.
 *
 * <p>Plain JUnit, no Quarkus: the gate has no CDI dependencies, all completion is driven manually
 * through {@link ControlledWork} so every test is deterministic.
 */
public class RerankingConcurrencyGateTest {

  private static final String PROVIDER = "nvidia";
  private static final String MODEL = "testModel";

  private SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  private RerankingConcurrencyGate gate(int maxConcurrentCalls, int maxQueuedCalls) {
    return new RerankingConcurrencyGate(
        PROVIDER, MODEL, maxConcurrentCalls, maxQueuedCalls, meterRegistry);
  }

  /** Work whose start and completion the test controls explicitly. */
  private static final class ControlledWork {
    private final AtomicReference<UniEmitter<? super String>> emitter = new AtomicReference<>();
    private final AtomicBoolean invoked = new AtomicBoolean();

    Uni<String> uni() {
      return Uni.createFrom()
          .emitter(
              em -> {
                invoked.set(true);
                emitter.set(em);
              });
    }

    boolean invoked() {
      return invoked.get();
    }

    void complete() {
      emitter.get().complete("done");
    }

    void fail(Throwable t) {
      emitter.get().fail(t);
    }
  }

  private UniAssertSubscriber<String> submit(RerankingConcurrencyGate gate, ControlledWork work) {
    return gate.withPermit(work::uni).subscribe().withSubscriber(UniAssertSubscriber.create());
  }

  @Test
  @DisplayName("fast path acquires immediately when capacity is free and nothing is queued")
  void fastPathAcquiresImmediately() {
    var gate = gate(2, 10);
    var work = new ControlledWork();

    var sub = submit(gate, work);

    assertThat(work.invoked()).as("work starts immediately when a slot is free").isTrue();
    assertThat(gate.inFlight()).isEqualTo(1);
    assertThat(gate.queueDepth()).isZero();

    work.complete();
    sub.assertCompleted().assertItem("done");
    assertThat(gate.inFlight()).isZero();
  }

  @Test
  @DisplayName("in-flight calls never exceed maxConcurrentCalls; excess is parked FIFO")
  void capNeverExceededAndFifoOrder() {
    var gate = gate(2, 10);
    List<ControlledWork> works = new ArrayList<>();
    List<UniAssertSubscriber<String>> subs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      var work = new ControlledWork();
      works.add(work);
      subs.add(submit(gate, work));
    }

    // only the first two run, three are parked
    assertThat(works.get(0).invoked()).isTrue();
    assertThat(works.get(1).invoked()).isTrue();
    assertThat(works.get(2).invoked()).isFalse();
    assertThat(works.get(3).invoked()).isFalse();
    assertThat(works.get(4).invoked()).isFalse();
    assertThat(gate.inFlight()).isEqualTo(2);
    assertThat(gate.queueDepth()).isEqualTo(3);

    // releasing one slot grants the parked waiters in FIFO order
    works.get(0).complete();
    assertThat(works.get(2).invoked()).as("waiter 2 granted first (FIFO)").isTrue();
    assertThat(works.get(3).invoked()).isFalse();
    assertThat(gate.inFlight()).isEqualTo(2);
    assertThat(gate.queueDepth()).isEqualTo(2);

    works.get(1).complete();
    assertThat(works.get(3).invoked()).as("waiter 3 granted second (FIFO)").isTrue();
    assertThat(works.get(4).invoked()).isFalse();

    works.get(2).complete();
    assertThat(works.get(4).invoked()).as("waiter 4 granted last (FIFO)").isTrue();
    works.get(3).complete();
    works.get(4).complete();

    subs.forEach(sub -> sub.assertCompleted().assertItem("done"));
    assertThat(gate.inFlight()).isZero();
    assertThat(gate.queueDepth()).isZero();
  }

  @Test
  @DisplayName("a new arrival cannot barge past parked waiters")
  void noBargingWhilstWaitersParked() {
    var gate = gate(1, 10);
    var holder = new ControlledWork();
    submit(gate, holder);

    var parked = new ControlledWork();
    submit(gate, parked);
    assertThat(parked.invoked()).isFalse();

    var late = new ControlledWork();
    submit(gate, late);

    holder.complete();
    assertThat(parked.invoked()).as("parked waiter wins over the later arrival").isTrue();
    assertThat(late.invoked()).isFalse();

    parked.complete();
    assertThat(late.invoked()).isTrue();
    late.complete();
  }

  @Test
  @DisplayName("queue overflow fails fast with RERANKING_PROVIDER_OVERLOADED, work never invoked")
  void overflowRejectsImmediately() {
    var gate = gate(1, 1);
    var holder = new ControlledWork();
    submit(gate, holder);
    var parked = new ControlledWork();
    submit(gate, parked);

    var rejectedWork = new ControlledWork();
    var rejectedSub = submit(gate, rejectedWork);

    var failure = rejectedSub.assertFailed().getFailure();
    assertThat(failure)
        .isInstanceOfSatisfying(
            RerankingProviderException.class,
            e ->
                assertThat(e.code)
                    .isEqualTo(
                        RerankingProviderException.Code.RERANKING_PROVIDER_OVERLOADED.name()));
    assertThat(rejectedWork.invoked()).as("rejected work must never start").isFalse();
    assertThat(gate.queueDepth()).isEqualTo(1);

    assertThat(
            meterRegistry
                .get(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_REJECTED_METRIC)
                .counter()
                .count())
        .isEqualTo(1.0);

    // the rejected request did not leak queue capacity: a slot frees, the parked one runs
    holder.complete();
    assertThat(parked.invoked()).isTrue();
    parked.complete();
    assertThat(gate.inFlight()).isZero();
    assertThat(gate.queueDepth()).isZero();
  }

  @Test
  @DisplayName("cancelling a parked waiter frees its queue slot without consuming a permit")
  void cancelWhileParkedFreesQueueSlot() {
    var gate = gate(1, 1);
    var holder = new ControlledWork();
    submit(gate, holder);

    var parked = new ControlledWork();
    var parkedSub = submit(gate, parked);
    assertThat(gate.queueDepth()).isEqualTo(1);

    parkedSub.cancel();
    assertThat(gate.queueDepth()).as("abandoned waiter releases queue capacity").isZero();

    // queue capacity is available again for a new request
    var next = new ControlledWork();
    submit(gate, next);
    assertThat(gate.queueDepth()).isEqualTo(1);

    holder.complete();
    assertThat(parked.invoked()).as("cancelled waiter must never start").isFalse();
    assertThat(next.invoked()).isTrue();
    next.complete();
    assertThat(gate.inFlight()).isZero();
  }

  @Test
  @DisplayName("work failure releases the permit and grants the next waiter")
  void workFailureReleasesPermit() {
    var gate = gate(1, 10);
    var failing = new ControlledWork();
    var failingSub = submit(gate, failing);
    var parked = new ControlledWork();
    var parkedSub = submit(gate, parked);

    failing.fail(new RuntimeException("provider blew up"));
    failingSub.assertFailed();

    assertThat(parked.invoked()).as("permit freed by the failure grants the waiter").isTrue();
    parked.complete();
    parkedSub.assertCompleted();
    assertThat(gate.inFlight()).isZero();
  }

  @Test
  @DisplayName("cancelling in-flight work releases the permit")
  void cancelInFlightWorkReleasesPermit() {
    var gate = gate(1, 10);
    var inFlightWork = new ControlledWork();
    var inFlightSub = submit(gate, inFlightWork);
    assertThat(gate.inFlight()).isEqualTo(1);

    inFlightSub.cancel();
    assertThat(gate.inFlight()).as("cancellation of running work frees the slot").isZero();

    var next = new ControlledWork();
    submit(gate, next);
    assertThat(next.invoked()).isTrue();
    next.complete();
  }

  @Test
  @DisplayName("gauges track queue depth and in-flight; wait timer records parked grants")
  void metersTrackGateState() {
    var gate = gate(1, 10);
    var holder = new ControlledWork();
    submit(gate, holder);
    var parked = new ControlledWork();
    submit(gate, parked);

    assertThat(
            meterRegistry
                .get(MetricsConstants.MetricNames.RERANK_ALL_INFLIGHT_CALLS_METRIC)
                .gauge()
                .value())
        .isEqualTo(1.0);
    assertThat(
            meterRegistry
                .get(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_DEPTH_METRIC)
                .gauge()
                .value())
        .isEqualTo(1.0);

    holder.complete();
    parked.complete();

    assertThat(
            meterRegistry
                .get(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_WAIT_DURATION_METRIC)
                .timer()
                .count())
        .as("one parked grant recorded in the wait timer")
        .isEqualTo(1);
    assertThat(
            meterRegistry
                .get(MetricsConstants.MetricNames.RERANK_ALL_INFLIGHT_CALLS_METRIC)
                .gauge()
                .value())
        .isZero();
  }

  @Test
  @DisplayName("cap holds under multi-threaded contention")
  void capHoldsUnderContention() throws Exception {
    final int maxConcurrent = 4;
    final int tasks = 200;
    var gate = gate(maxConcurrent, tasks);
    var running = new AtomicInteger();
    var maxObserved = new AtomicInteger();
    var completed = new CountDownLatch(tasks);

    ExecutorService pool = Executors.newFixedThreadPool(16);
    try {
      for (int i = 0; i < tasks; i++) {
        pool.submit(
            () ->
                gate.withPermit(
                        () ->
                            Uni.createFrom()
                                .item("ok")
                                .onItem()
                                .invoke(
                                    ignored -> {
                                      int now = running.incrementAndGet();
                                      maxObserved.accumulateAndGet(now, Math::max);
                                      // hold the slot briefly so overlap is observable
                                      try {
                                        Thread.sleep(1);
                                      } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                      }
                                      running.decrementAndGet();
                                    }))
                    .subscribe()
                    .with(item -> completed.countDown(), failure -> completed.countDown()));
      }
      assertThat(completed.await(30, TimeUnit.SECONDS)).isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(maxObserved.get())
        .as("observed concurrency must never exceed the configured cap")
        .isLessThanOrEqualTo(maxConcurrent);
    assertThat(gate.inFlight()).isZero();
    assertThat(gate.queueDepth()).isZero();
  }
}
