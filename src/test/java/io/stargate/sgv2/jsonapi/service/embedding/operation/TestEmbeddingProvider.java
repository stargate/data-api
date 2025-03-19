package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankingConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import java.util.ArrayList;
import java.util.List;

public class TestEmbeddingProvider extends EmbeddingProvider {

  public static CommandContext<CollectionSchemaObject> commandContextWithVectorize =
      TestConstants.collectionContext(
          "testCommand",
          new CollectionSchemaObject(
              TestConstants.SCHEMA_OBJECT_NAME,
              null,
              IdConfig.defaultIdConfig(),
              VectorConfig.fromColumnDefinitions(
                  List.of(
                      new VectorColumnDefinition(
                          DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                          3,
                          SimilarityFunction.COSINE,
                          EmbeddingSourceModel.OTHER,
                          new VectorizeDefinition("custom", "custom", null, null)))),
              null,
              CollectionLexicalConfig.configForDisabled(),
              CollectionRerankingConfig.configForLegacyCollections()),
          null,
          new TestEmbeddingProvider());

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
    return Uni.createFrom().item(new Response(batchId, response, new VectorizeUsage()));
  }

  @Override
  public int maxBatchSize() {
    return 1;
  }
}
