package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestEmbeddingProvider implements EmbeddingProvider {

  public static CommandContext commandContextWithVectorize =
      new CommandContext(
          "namespace",
          "collection",
          new CollectionSettings(
              "collection",
              true,
              3,
              CollectionSettings.SimilarityFunction.COSINE,
              null,
              null,
              null),
          new TestEmbeddingProvider(),
          "testCommand",
          null);

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts, Optional<String> apiKey, EmbeddingRequestType embeddingRequestType) {
    List<float[]> response = new ArrayList<>(texts.size());
    texts.forEach(t -> response.add(new float[] {0.25f, 0.25f, 0.25f}));
    return Uni.createFrom().item(response);
  }
}
