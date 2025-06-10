package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import java.util.ArrayList;
import java.util.List;

/** Mock a test reranking provider that returns ranks based on query and passages */
public class TestRerankingProvider extends RerankingProvider {

  // TODO: XXX Remove if not needed
  //  protected TestRerankingProvider(
  //      String baseUrl,
  //      String modelName,
  //      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
  //          requestProperties) {
  //    super(baseUrl, modelName, requestProperties);
  //  }

  protected TestRerankingProvider(int maxBatchSize) {
    super(
        ModelProvider.CUSTOM,
        "mockUrl",
        "mockModel",
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
            .RequestPropertiesImpl(3, 100, 5000, 500, 0.5, maxBatchSize));
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
    return Uni.createFrom().item(new BatchedRerankingResponse(batchId, ranks, ModelUsage.EMPTY));
  }
}
