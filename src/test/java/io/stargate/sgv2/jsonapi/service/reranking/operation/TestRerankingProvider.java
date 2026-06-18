package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

  protected TestRerankingProvider(int maxBatchSize) {
    super(
        ModelProvider.CUSTOM,
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
            "testModel",
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
            false,
            "http://testing.com",
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
                .RequestPropertiesImpl(3, 100, 5000, 500, 0.5, maxBatchSize)));
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used in tests
    return "";
  }

  @Override
  public Uni<BatchedRerankingResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankCredentials) {

    List<Rank> ranks = new ArrayList<>(passages.size());
    for (int i = 0; i < passages.size(); i++) {
      String passage = passages.get(i);
      float score = passage.equals(query) ? 1.0f : (float) Math.random(); // Example scoring logic
      ranks.add(new Rank(i, score));
    }

    ranks.sort((o1, o2) -> Float.compare(o2.score(), o1.score())); // Descending order
    return Uni.createFrom()
        .item(
            new BatchedRerankingResponse(batchId, ranks, createEmptyModelUsage(rerankCredentials)));
  }
}
