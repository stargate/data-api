package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

public class TestEmbeddingProvider implements EmbeddingProvider {

  public static CommandContext commandContextWithVectorize =
      new CommandContext(
          "namespace",
          "collection",
          new CollectionSettings(
              "collections",
              CollectionSettings.IdConfig.defaultIdConfig(),
              new CollectionSettings.VectorConfig(
                  true,
                  3,
                  CollectionSettings.SimilarityFunction.COSINE,
                  new CollectionSettings.VectorConfig.VectorizeConfig(
                      "custom", "custom", null, null)),
              null),
          new TestEmbeddingProvider(),
          "testCommand",
          null);

  @Override
  public Uni<Pair<Integer, List<float[]>>> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKey,
      EmbeddingRequestType embeddingRequestType) {
    List<float[]> response = new ArrayList<>(texts.size());
    texts.forEach(
        t -> {
          if (t.equals("return 1s")) response.add(new float[] {1.0f, 1.0f, 1.0f});
          else response.add(new float[] {0.25f, 0.25f, 0.25f});
        });
    return Uni.createFrom().item(Pair.of(batchId, response));
  }

  @Override
  public int batchSize() {
    return 1;
  }
}
