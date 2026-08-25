package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Mock a test reranking provider that returns ranks based on query and passages */
public class TestRerankingProvider extends RerankingProvider {

  private static final TestConstants testConstants = new TestConstants();

  private static final RerankingCredentials RERANK_CREDENTIALS =
      new RerankingCredentials(testConstants.TENANT, "mocked reranking api key");

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

  private static final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig MODEL_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
          "testModel",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          false,
          "http://testing.com",
          REQUEST_PROPERTIES);

  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl PROVIDER_CONFIG =
      new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
          false, "test", true, Map.of(), List.of());

  /** Tracks how many batch calls run at once, shareable between providers in a test. */
  public static final class ConcurrencyTracker {
    private final AtomicInteger current = new AtomicInteger();
    private final AtomicInteger max = new AtomicInteger();

    void enter() {
      max.accumulateAndGet(current.incrementAndGet(), Math::max);
    }

    void exit() {
      current.decrementAndGet();
    }

    public int maxObserved() {
      return max.get();
    }

    public int current() {
      return current.get();
    }
  }

  private final Duration batchDelay;
  private final ConcurrencyTracker tracker;
  private final AtomicInteger invokedBatches = new AtomicInteger();

  protected TestRerankingProvider(int maxBatchSize) {
    this(
        modelConfig(maxBatchSize, 8, 30000),
        permissiveGate(),
        Duration.ZERO,
        new ConcurrencyTracker());
  }

  protected TestRerankingProvider(
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig,
      RerankingConcurrencyGate gate,
      Duration batchDelay,
      ConcurrencyTracker tracker) {
    super(ModelProvider.CUSTOM, modelConfig, gate);
    this.batchDelay = batchDelay;
    this.tracker = tracker;
  }

  /** Builds a model config for tests that need specific batching/deadline properties. */
  static RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig(
      int maxBatchSize, int maxConcurrentBatches, int totalTimeoutMillis) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
        "testModel",
        new ApiModelSupport.ApiModelSupportImpl(
            ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
        false,
        "http://testing.com",
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
            .RequestPropertiesImpl(
            3,
            100,
            5000,
            500,
            0.5,
            maxBatchSize,
            maxConcurrentBatches,
            32,
            1000,
            totalTimeoutMillis));
  }

  /** A gate large enough that it never interferes with single-provider batching tests. */
  public static RerankingConcurrencyGate permissiveGate() {
    return new RerankingConcurrencyGate(
        ModelProvider.CUSTOM.apiName(), "testModel", 32, 1000, new SimpleMeterRegistry());
  }

  /** How many batch calls actually started, e.g. to prove a rejected request never ran. */
  public int invokedBatches() {
    return invokedBatches.get();
  }

  public ConcurrencyTracker tracker() {
    return tracker;
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used in tests
    return "";
  }

  @Override
  public Uni<BatchedRerankingResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankCredentials) {

    invokedBatches.incrementAndGet();
    return Uni.createFrom()
        .deferred(
            () -> {
              var started = new AtomicBoolean();
              Uni<BatchedRerankingResponse> result =
                  Uni.createFrom()
                      .item(
                          () -> {
                            started.set(true);
                            tracker.enter();
                            return respond(batchId, query, passages, rerankCredentials);
                          });
              if (!batchDelay.isZero()) {
                result = result.onItem().delayIt().by(batchDelay);
              }
              return result
                  .onTermination()
                  .invoke(
                      () -> {
                        if (started.get()) {
                          tracker.exit();
                        }
                      });
            });
  }

  private BatchedRerankingResponse respond(
      int batchId, String query, List<String> passages, RerankingCredentials rerankCredentials) {
    List<Rank> ranks = new ArrayList<>(passages.size());
    for (int i = 0; i < passages.size(); i++) {
      String passage = passages.get(i);
      float score = passage.equals(query) ? 1.0f : (float) Math.random(); // Example scoring logic
      ranks.add(new Rank(i, score));
    }

    ranks.sort((o1, o2) -> Float.compare(o2.score(), o1.score())); // Descending order
    return new BatchedRerankingResponse(batchId, ranks, createEmptyModelUsage(rerankCredentials));
  }
}
