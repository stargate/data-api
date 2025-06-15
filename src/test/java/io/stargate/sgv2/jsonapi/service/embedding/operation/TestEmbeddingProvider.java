package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.mockito.Mockito.mock;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestEmbeddingProvider extends EmbeddingProvider {

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig
      TEST_MODEL_CONFIG =
      new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
          "testModel",
          new ApiModelSupport.ApiModelSupportImpl(
              ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
          Optional.empty(),
          List.of(),
          Map.of(),
          Optional.empty());

  private static final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl REQUEST_PROPERTIES =  new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.RequestPropertiesImpl(
      3,10,100,100,0.5, Optional.empty(), Optional.empty(), Optional.empty(), 10);

  private static final EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl PROVIDER_CONFIG = new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl(
      ModelProvider.CUSTOM.apiName(),
      true,
      Optional.of("http://testing.com"),
      false,
      Map.of(), List.of(), REQUEST_PROPERTIES, List.of());


  public static final TestEmbeddingProvider TEST_EMBEDDING_PROVIDER =
      new TestEmbeddingProvider();

  public TestEmbeddingProvider(){
    super(
        ModelProvider.CUSTOM,
        PROVIDER_CONFIG,
        "http://testing.com",
        TEST_MODEL_CONFIG,
        3,
        Map.of());
  }

  public CommandContext<CollectionSchemaObject> commandContextWithVectorize() {
    return TEST_CONSTANTS.collectionContext(
        "testCommand",
        new CollectionSchemaObject(
            TEST_CONSTANTS.SCHEMA_OBJECT_NAME,
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
            CollectionRerankDef.configForPreRerankingCollection()),
        null,
        TEST_EMBEDDING_PROVIDER);
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used in tests
    return "";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
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

    var modelUsage =
        createModelUsage(
            embeddingCredentials.tenantId(),
            ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
            0,
            0,
            0,
            0,
            0);
    return Uni.createFrom().item(new BatchedEmbeddingResponse(batchId, response, modelUsage));
  }

  @Override
  public int maxBatchSize() {
    return 1;
  }
}
