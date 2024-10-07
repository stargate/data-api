package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddingProvider extends EmbeddingProvider {

  public static CommandContext<CollectionSchemaObject> commandContextWithVectorize =
      new CommandContext<>(
          new CollectionSchemaObject(
              TestConstants.SCHEMA_OBJECT_NAME,
              null,
              IdConfig.defaultIdConfig(),
              List.of(
                  new VectorConfig(
                      true,
                          DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                      3,
                      SimilarityFunction.COSINE,
                      new VectorConfig.VectorizeConfig("custom", "custom", null, null))),
              null),
          new TestEmbeddingProvider(),
          "testCommand",
          null,
          TestConstants.DEFAULT_API_FEATURES_FOR_TESTS);

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
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
