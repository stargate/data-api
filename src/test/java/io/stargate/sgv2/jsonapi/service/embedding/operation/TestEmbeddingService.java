package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddingService implements EmbeddingService {

  public static CommandContext commandContextWithVectorize =
      new CommandContext(
          "namespace",
          "collection",
          new CollectionSettings(
              "collections",
              CollectionSettings.IdConfig.defaultIdConfig(),
              new CollectionSettings.VectorConfig(
                  true, 3, CollectionSettings.SimilarityFunction.COSINE, null),
              null),
          new TestEmbeddingService(),
          "testCommand",
          null);

  @Override
  public List<float[]> vectorize(List<String> texts) {
    List<float[]> response = new ArrayList<>(texts.size());
    texts.forEach(t -> response.add(new float[] {0.25f, 0.25f, 0.25f}));
    return response;
  }
}
