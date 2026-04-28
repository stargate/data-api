package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderTest {

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants testConstants = new TestConstants();

  @Test
  void keepsExplicitHybridLimitsAtMaximumPageSizeOnCommandContext() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": { "$vector": 100, "$lexical": 25 }
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContext)
        .withCommand(command)
        .withFindCommandResolver(findCommandResolver)
        .build();

    assertThat(commandContext.getHybridLimits().vectorLimit())
        .isEqualTo(OperationsConfig.MAX_HYBRID_SEARCH_LIMIT);
    assertThat(commandContext.getHybridLimits().lexicalLimit()).isEqualTo(25);
  }

  private FindAndRerankCommand command(String json) throws Exception {
    return objectMapper.readValue(json, FindAndRerankCommand.class);
  }

  private CommandContext<CollectionSchemaObject> commandContext() {
    var commandContext =
        testConstants.collectionContext("findAndRerank", vectorLexicalRerankSchema(), null, null);

    var rerankingProvidersConfig = mock(RerankingProvidersConfig.class);
    var modelConfig = mock(RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.class);
    when(modelConfig.apiModelSupport())
        .thenReturn(
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()));
    when(rerankingProvidersConfig.filterByRerankServiceDef(any())).thenReturn(modelConfig);
    when(commandContext.rerankingProviderFactory().getRerankingConfig())
        .thenReturn(rerankingProvidersConfig);
    when(commandContext.rerankingProviderFactory().create(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(RerankingProvider.class));

    return commandContext;
  }

  private CollectionSchemaObject vectorLexicalRerankSchema() {
    return new CollectionSchemaObject(
        testConstants.COLLECTION_IDENTIFIER,
        IdConfig.defaultIdConfig(),
        VectorConfig.fromColumnDefinitions(
            List.of(
                new VectorColumnDefinition(
                    DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                    -1,
                    SimilarityFunction.COSINE,
                    EmbeddingSourceModel.OTHER,
                    null))),
        null,
        CollectionLexicalConfig.configForDefault(),
        new CollectionRerankDef(
            true,
            new CollectionRerankDef.RerankServiceDef(
                "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null)));
  }
}
