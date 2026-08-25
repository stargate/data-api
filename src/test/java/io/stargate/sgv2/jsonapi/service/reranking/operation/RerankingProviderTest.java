package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RerankingProviderTest {

  private static final TestConstants testConstants = new TestConstants();

  private static final RerankingCredentials RERANK_CREDENTIALS =
      new RerankingCredentials(testConstants.TENANT, "mocked reranking api key");

  @Test
  @SuppressWarnings("unchecked")
  void createPassageBatchesTest() {
    // mock a test reranking provider with maxBatchSize configured to 3
    TestRerankingProvider mockRerankingProvider = new TestRerankingProvider(3);
    // mock 11 passages
    List<String> passages =
        List.of(
            "orange",
            "apple",
            "banana",
            "grape",
            "kiwi",
            "mango",
            "pear",
            "peach",
            "plum",
            "pineapple",
            "strawberry");

    // invoke the private method createPassageBatches
    try {
      java.lang.reflect.Method method =
          RerankingProvider.class.getDeclaredMethod("createPassageBatches", List.class);
      method.setAccessible(true);
      List<List<String>> batches =
          (List<List<String>>) method.invoke(mockRerankingProvider, passages);
      assertThat(batches).hasSize(4);
      assertThat(batches.get(0)).hasSize(3);
      assertThat(batches.get(1)).hasSize(3);
      assertThat(batches.get(2)).hasSize(3);
      assertThat(batches.get(3)).hasSize(2);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void microBatchingTest() {
    // mock a test reranking provider with maxBatchSize configured to 10
    TestRerankingProvider mockRerankingProvider = new TestRerankingProvider(10);

    // mock query string
    String query = "apple";
    // mock 15 passages, which will be split into 2 micro batches
    List<String> passages =
        List.of(
            "orange",
            "apple",
            "banana",
            "grape",
            "kiwi",
            "mango",
            "pear",
            "peach",
            "plum",
            "pineapple",
            "strawberry",
            "blueberry",
            "raspberry",
            "watermelon",
            "cherry");

    final RerankingProvider.RerankingResponse finalResult =
        mockRerankingProvider
            .rerank(query, passages, RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // check if the final result contains all 15 passages
    assertThat(finalResult.ranks().size()).isEqualTo(passages.size());
    // assert the order of the passages in the final result should be the same as the original
    // passage order
    IntStream.range(0, 15)
        .forEach(i -> assertThat(finalResult.ranks().get(i).index()).isEqualTo(i));
  }

  private static List<String> passages(int count) {
    return IntStream.range(0, count).mapToObj("passage-%d"::formatted).toList();
  }

  private static RerankingConcurrencyGate gate(int maxConcurrentCalls, int maxQueuedCalls) {
    return new RerankingConcurrencyGate(
        "custom", "testModel", maxConcurrentCalls, maxQueuedCalls, new SimpleMeterRegistry());
  }

  /** The gate releases permits during cancellation cleanup; give async termination a moment. */
  private static void awaitGateIdle(RerankingConcurrencyGate gate) {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while ((gate.inFlight() != 0 || gate.queueDepth() != 0) && System.nanoTime() < deadline) {
      Thread.onSpinWait();
    }
    assertThat(gate.inFlight()).isZero();
    assertThat(gate.queueDepth()).isZero();
  }

  @Test
  @DisplayName("per-request fan-out cap bounds concurrent batches; aggregation stays correct")
  void fanOutCapLimitsConcurrentBatches() {
    // 100 passages / batch size 10 = 10 batches, but at most 3 in flight at once
    var provider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 3, 30000),
            TestRerankingProvider.permissiveGate(),
            Duration.ofMillis(50),
            new TestRerankingProvider.ConcurrencyTracker());
    List<String> passages = passages(100);

    var finalResult =
        provider
            .rerank("query", passages, RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem(Duration.ofSeconds(30))
            .getItem();

    assertThat(provider.invokedBatches()).isEqualTo(10);
    assertThat(provider.tracker().maxObserved())
        .as("concurrent batch calls must not exceed max-concurrent-batches")
        .isLessThanOrEqualTo(3);
    assertThat(finalResult.ranks()).hasSize(passages.size());
    IntStream.range(0, passages.size())
        .forEach(i -> assertThat(finalResult.ranks().get(i).index()).isEqualTo(i));
  }

  @Test
  @DisplayName("a shared gate bounds in-flight calls across provider instances (per-pod bulkhead)")
  void sharedGateBoundsConcurrencyAcrossProviders() {
    var sharedGate = gate(1, 1000);
    var sharedTracker = new TestRerankingProvider.ConcurrencyTracker();
    var provider1 =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 30000),
            sharedGate,
            Duration.ofMillis(50),
            sharedTracker);
    var provider2 =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 30000),
            sharedGate,
            Duration.ofMillis(50),
            sharedTracker);

    var sub1 =
        provider1
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());
    var sub2 =
        provider2
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    sub1.awaitItem(Duration.ofSeconds(10));
    sub2.awaitItem(Duration.ofSeconds(10));
    assertThat(sharedTracker.maxObserved())
        .as("providers created per-request must share the per-pod cap")
        .isEqualTo(1);
    awaitGateIdle(sharedGate);
  }

  @Test
  @DisplayName("total deadline fails a parked request without ever calling the provider")
  void deadlineWhileParkedNeverInvokesWork() {
    var sharedGate = gate(1, 1000);
    // occupies the only permit for ~10s
    var slowProvider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 30000),
            sharedGate,
            Duration.ofSeconds(10),
            new TestRerankingProvider.ConcurrencyTracker());
    var slowSub =
        slowProvider
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    // parks behind the slow call, its 150ms total deadline expires while waiting
    var parkedProvider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 150),
            sharedGate,
            Duration.ZERO,
            new TestRerankingProvider.ConcurrencyTracker());
    var parkedSub =
        parkedProvider
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    var failure = parkedSub.awaitFailure(Duration.ofSeconds(10)).getFailure();
    assertThat(failure)
        .isInstanceOfSatisfying(
            RerankingProviderException.class,
            e -> {
              assertThat(e.code)
                  .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
              assertThat(e.getMessage()).contains("150");
            });
    assertThat(parkedProvider.invokedBatches())
        .as("a request that timed out while parked must never reach the provider")
        .isZero();

    slowSub.cancel();
    awaitGateIdle(sharedGate);
  }

  @Test
  @DisplayName("total deadline fires mid-call and the cancelled call releases its permit")
  void deadlineWhileInFlightReleasesPermit() {
    var sharedGate = gate(1, 1000);
    var provider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 150),
            sharedGate,
            Duration.ofSeconds(10),
            new TestRerankingProvider.ConcurrencyTracker());

    var failure =
        provider
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure(Duration.ofSeconds(10))
            .getFailure();

    assertThat(failure)
        .isInstanceOfSatisfying(
            RerankingProviderException.class,
            e ->
                assertThat(e.code)
                    .isEqualTo(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name()));
    awaitGateIdle(sharedGate);
  }

  @Test
  @DisplayName("gate overflow surfaces RERANKING_PROVIDER_OVERLOADED to the caller")
  void queueOverflowSurfacesOverloaded() {
    var sharedGate = gate(1, 0); // no queueing at all
    var slowProvider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 30000),
            sharedGate,
            Duration.ofSeconds(10),
            new TestRerankingProvider.ConcurrencyTracker());
    var slowSub =
        slowProvider
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    var rejectedProvider =
        new TestRerankingProvider(
            TestRerankingProvider.modelConfig(10, 8, 30000),
            sharedGate,
            Duration.ZERO,
            new TestRerankingProvider.ConcurrencyTracker());
    var failure =
        rejectedProvider
            .rerank("query", passages(10), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure(Duration.ofSeconds(10))
            .getFailure();

    assertThat(failure)
        .isInstanceOfSatisfying(
            RerankingProviderException.class,
            e ->
                assertThat(e.code)
                    .isEqualTo(
                        RerankingProviderException.Code.RERANKING_PROVIDER_OVERLOADED.name()));
    assertThat(rejectedProvider.invokedBatches()).isZero();

    slowSub.cancel();
    awaitGateIdle(sharedGate);
  }

  @Test
  @DisplayName("OVERLOADED is never retried; provider TIMEOUT stays retryable")
  void overloadedIsNotRetried() {
    var provider = new TestRerankingProvider(10);

    var overloaded =
        RerankingProviderException.Code.RERANKING_PROVIDER_OVERLOADED.get(
            Map.of(
                "modelProvider", "custom",
                "modelName", "testModel",
                "maxConcurrentCalls", "1",
                "maxQueuedCalls", "0"));
    assertThat(provider.decideRetry(overloaded))
        .as("retrying an overloaded rejection would deepen the overload")
        .isFalse();

    var timeout =
        RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.get(
            Map.of(
                "modelProvider", "custom",
                "httpStatus", "504",
                "errorMessage", "upstream timeout"));
    assertThat(provider.decideRetry(timeout)).isTrue();
  }
}
