package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TestEmbeddingProvider extends EmbeddingProvider {

  public static CommandContext commandContextWithVectorize =
      new CommandContext(
          new CollectionSchemaObject(
              "namespace",
              "collections",
              CollectionSchemaObject.IdConfig.defaultIdConfig(),
              new CollectionSchemaObject.VectorConfig(
                  true,
                  3,
                  CollectionSchemaObject.SimilarityFunction.COSINE,
                  new CollectionSchemaObject.VectorConfig.VectorizeConfig(
                      "custom", "custom", null, null)),
              null),
          new TestEmbeddingProvider(),
          "testCommand",
          null);

  @Override
  public Uni<Response> vectorize(
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
    return Uni.createFrom().item(Response.of(batchId, response));
  }

  @Override
  public int maxBatchSize() {
    return 1;
  }
}
