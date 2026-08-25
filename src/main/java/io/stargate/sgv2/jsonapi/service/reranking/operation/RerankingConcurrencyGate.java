package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.metrics.MetricsConstants;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A non-blocking bulkhead for reranking provider calls, shared by every request on this pod for one
 * provider+model pair.
 *
 * <p>At most {@code maxConcurrentCalls} reranking calls run at once; up to {@code maxQueuedCalls}
 * further calls park in a FIFO queue. When both limits are reached, new calls fail immediately with
 * {@link RerankingProviderException.Code#RERANKING_PROVIDER_OVERLOADED} without ever reaching the
 * provider. Cancelling a caller (e.g. the total rerank deadline firing) while it is parked frees
 * its queue slot without consuming a permit.
 *
 * <p>Providers are constructed per API request (see {@link RerankingProviderFactory}), so instances
 * of this class must be shared — obtain them from {@link RerankingConcurrencyGateRegistry}.
 *
 * <p>Never blocks a thread: acquisition either completes synchronously, fails synchronously, or
 * parks a {@link UniEmitter} that is completed later by whichever thread releases a permit.
 */
public class RerankingConcurrencyGate {

  private final String providerName;
  private final String modelName;
  private final int maxConcurrentCalls;
  private final int maxQueuedCalls;

  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicInteger queued = new AtomicInteger();
  private final ConcurrentLinkedQueue<Waiter> waiters = new ConcurrentLinkedQueue<>();

  private final Timer queueWaitTimer;
  private final Counter rejectedCounter;

  public RerankingConcurrencyGate(
      String providerName,
      String modelName,
      int maxConcurrentCalls,
      int maxQueuedCalls,
      MeterRegistry meterRegistry) {
    if (maxConcurrentCalls <= 0) {
      throw new IllegalArgumentException(
          "maxConcurrentCalls must be positive, got " + maxConcurrentCalls);
    }
    if (maxQueuedCalls < 0) {
      throw new IllegalArgumentException("maxQueuedCalls must be >= 0, got " + maxQueuedCalls);
    }
    this.providerName = providerName;
    this.modelName = modelName;
    this.maxConcurrentCalls = maxConcurrentCalls;
    this.maxQueuedCalls = maxQueuedCalls;

    var tags =
        Tags.of(
            MetricsConstants.MetricTags.RERANKING_PROVIDER_TAG, providerName,
            MetricsConstants.MetricTags.RERANKING_MODEL_TAG, modelName);
    // strongReference: the gate outlives any single request, the registry must not GC the gauges
    Gauge.builder(
            MetricsConstants.MetricNames.RERANK_ALL_INFLIGHT_CALLS_METRIC, inFlight::doubleValue)
        .tags(tags)
        .strongReference(true)
        .register(meterRegistry);
    Gauge.builder(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_DEPTH_METRIC, queued::doubleValue)
        .tags(tags)
        .strongReference(true)
        .register(meterRegistry);
    this.queueWaitTimer =
        Timer.builder(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_WAIT_DURATION_METRIC)
            .tags(tags)
            .register(meterRegistry);
    this.rejectedCounter =
        Counter.builder(MetricsConstants.MetricNames.RERANK_ALL_QUEUE_REJECTED_METRIC)
            .tags(tags)
            .register(meterRegistry);
  }

  /** Current number of in-flight calls holding a permit. Exposed for gauges and tests. */
  public int inFlight() {
    return inFlight.get();
  }

  /** Current number of parked waiters. Exposed for gauges and tests. */
  public int queueDepth() {
    return queued.get();
  }

  /**
   * Runs {@code work} once a permit is available, parking FIFO behind earlier callers when the gate
   * is full. The permit is released exactly once when the work emits an item, fails, or is
   * cancelled. Fails with {@code RERANKING_PROVIDER_OVERLOADED} when the wait queue is full; {@code
   * work} is then never invoked.
   */
  public <T> Uni<T> withPermit(Supplier<Uni<? extends T>> work) {
    return acquire()
        .onItem()
        .transformToUni(
            permit -> {
              permit.markConsumed();
              return Uni.createFrom().<T>deferred(work).onTermination().invoke(permit::release);
            });
  }

  private Uni<Permit> acquire() {
    return Uni.createFrom()
        .emitter(
            emitter -> {
              // fast path only when nobody is parked, so new arrivals cannot barge past waiters
              if (waiters.isEmpty() && tryReserveSlot()) {
                var permit = new Permit();
                // covers the race where the caller is cancelled between the grant and the
                // downstream consuming it: an unconsumed permit must not leak the slot
                emitter.onTermination(permit::releaseIfNotConsumed);
                emitter.complete(permit);
                return;
              }

              if (queued.incrementAndGet() > maxQueuedCalls) {
                queued.decrementAndGet();
                rejectedCounter.increment();
                emitter.fail(overloadedException());
                return;
              }

              var waiter = new Waiter(emitter);
              emitter.onTermination(waiter::onCallerTerminated);
              waiters.add(waiter);
              // a permit may have been released between the fast-path check and parking
              drain();
            });
  }

  private void release() {
    inFlight.decrementAndGet();
    drain();
  }

  /** Grants permits to parked waiters, oldest first, while capacity is available. */
  private void drain() {
    while (true) {
      if (waiters.isEmpty() || !tryReserveSlot()) {
        return;
      }
      // holding one reserved slot; find the oldest waiter not already abandoned
      Waiter granted = null;
      Waiter candidate;
      while ((candidate = waiters.poll()) != null) {
        // the permit is attached BEFORE the claim CAS: a concurrent cancellation that loses
        // the claim race must be able to see it and free the slot (see onCallerTerminated)
        candidate.prepareGrant(new Permit());
        if (candidate.tryClaimForGrant()) {
          granted = candidate;
          break;
        }
        // abandoned waiter: its queue accounting was already reverted, keep polling
      }
      if (granted == null) {
        // no live waiter for the reserved slot: put it back and re-check, another caller
        // may have parked concurrently
        inFlight.decrementAndGet();
        if (waiters.isEmpty()) {
          return;
        }
        continue;
      }
      queued.decrementAndGet();
      queueWaitTimer.record(System.nanoTime() - granted.enqueuedNanos, TimeUnit.NANOSECONDS);
      granted.grant();
    }
  }

  private boolean tryReserveSlot() {
    while (true) {
      int current = inFlight.get();
      if (current >= maxConcurrentCalls) {
        return false;
      }
      if (inFlight.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  private RerankingProviderException overloadedException() {
    return RerankingProviderException.Code.RERANKING_PROVIDER_OVERLOADED.get(
        Map.of(
            "modelProvider",
            providerName,
            "modelName",
            modelName,
            "maxConcurrentCalls",
            String.valueOf(maxConcurrentCalls),
            "maxQueuedCalls",
            String.valueOf(maxQueuedCalls)));
  }

  /**
   * One reserved slot. Consumed by the downstream continuation in {@link #withPermit(Supplier)};
   * released exactly once, either by the work's termination or — when the caller was cancelled
   * before the grant reached it — by the emitter's termination callback.
   */
  private final class Permit {
    private final AtomicBoolean consumed = new AtomicBoolean();
    private final AtomicBoolean released = new AtomicBoolean();

    void markConsumed() {
      consumed.set(true);
    }

    void release() {
      if (released.compareAndSet(false, true)) {
        RerankingConcurrencyGate.this.release();
      }
    }

    void releaseIfNotConsumed() {
      if (!consumed.get()) {
        release();
      }
    }
  }

  /**
   * A parked caller. The {@code claimed} flag is CAS-raced between the granting side ({@link
   * #drain()}) and the abandoning side (caller cancelled while parked) so exactly one wins.
   */
  private final class Waiter {
    private final UniEmitter<? super Permit> emitter;
    private final AtomicBoolean claimed = new AtomicBoolean();
    private final AtomicReference<Permit> grantedPermit = new AtomicReference<>();
    private final long enqueuedNanos = System.nanoTime();

    Waiter(UniEmitter<? super Permit> emitter) {
      this.emitter = emitter;
    }

    void prepareGrant(Permit permit) {
      grantedPermit.set(permit);
    }

    boolean tryClaimForGrant() {
      return claimed.compareAndSet(false, true);
    }

    void grant() {
      emitter.complete(grantedPermit.get());
    }

    /**
     * Runs when the caller's subscription terminates: after a successful grant delivery, or on
     * cancellation (deadline fired / client disconnected) whether parked or mid-grant.
     */
    void onCallerTerminated() {
      if (claimed.compareAndSet(false, true)) {
        // abandoned before any grant: free the queue capacity, no permit was consumed.
        // drain() lazily skips this node when polling.
        queued.decrementAndGet();
        return;
      }
      // the grant won the race; if it never reached the downstream continuation
      // (cancelled in between), the slot must be freed here
      var permit = grantedPermit.get();
      if (permit != null) {
        permit.releaseIfNotConsumed();
      }
    }
  }
}
