package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.bridge.executor.NamespaceCache;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddingService implements EmbeddingService {

  public static CommandContext commandContextWithVectorize =
      new CommandContext(
          "namespace",
          "collection",
          true,
          NamespaceCache.CollectionProperty.SimilarityFunction.COSINE,
          new TestEmbeddingService());

  @Override
  public List<float[]> vectorize(List<String> texts) {
    List<float[]> response = new ArrayList<>(texts.size());
    for (String text : texts) {
      response.add(new float[] {0.25f, 0.25f, 0.25f});
    }
    return response;
  }
}
