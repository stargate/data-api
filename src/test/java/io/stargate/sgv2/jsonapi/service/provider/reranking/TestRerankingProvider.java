package io.stargate.sgv2.jsonapi.service.provider.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.provider.reranking.operation.RerankingProvider;
import java.util.ArrayList;
import java.util.List;

/** Mock a test reranking provider that returns ranks based on query and passages */
public class TestRerankingProvider extends RerankingProvider {

  protected TestRerankingProvider(
      String baseUrl,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
          requestProperties) {
    super(baseUrl, modelName, requestProperties);
  }

  protected TestRerankingProvider(int maxBatchSize) {
    super(
        "mockUrl",
        "mockModel",
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
            .RequestPropertiesImpl(3, 100, 5000, 500, 0.5, maxBatchSize));
  }

  @Override
  public Uni<RerankingBatchResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankCredentials) {
    List<Rank> ranks = new ArrayList<>(passages.size());
    for (int i = 0; i < passages.size(); i++) {
      String passage = passages.get(i);
      float score = passage.equals(query) ? 1.0f : (float) Math.random(); // Example scoring logic
      ranks.add(new Rank(i, score));
    }
    ranks.sort((o1, o2) -> Float.compare(o2.score(), o1.score())); // Descending order
    return Uni.createFrom().item(RerankingBatchResponse.of(batchId, ranks, new Usage(0, 0)));
  }
}
