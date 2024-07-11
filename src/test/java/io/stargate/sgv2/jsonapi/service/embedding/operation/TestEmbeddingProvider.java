package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddingProvider extends EmbeddingProvider {

  public static CommandContext<CollectionSchemaObject> commandContextWithVectorize =
      new CommandContext<>(
          new CollectionSchemaObject(
              TestConstants.SCHEMA_OBJECT_NAME,
              CollectionSchemaObject.IdConfig.defaultIdConfig(),
              new VectorConfig(
                  true,
                  3,
                  CollectionSchemaObject.SimilarityFunction.COSINE,
                  new VectorConfig.VectorizeConfig("custom", "custom", null, null)),
              null),
          new TestEmbeddingProvider(),
          "testCommand",
          null);

  @Override
  public Uni<Response> vectorize(
      int batchId, List<String> texts, String apiKey, EmbeddingRequestType embeddingRequestType) {
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
