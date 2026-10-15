package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static io.stargate.sgv2.jsonapi.service.resolver.FindAndRerankOperationAssertions.innerTaskGroups;
import static io.stargate.sgv2.jsonapi.service.resolver.FindAndRerankOperationAssertions.privateField;
import static io.stargate.sgv2.jsonapi.service.resolver.FindAndRerankOperationAssertions.readTaskSnapshots;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTask;
import io.stargate.sgv2.jsonapi.service.operation.reranking.DeferredVectorize;
import io.stargate.sgv2.jsonapi.service.operation.reranking.IntermediateCollectionReadTask;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.resolver.FindAndRerankOperationAssertions.ReadTaskSnapshot;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderTest {

  // @QuarkusTest needed for error template initialization
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  /** Helper that calls the centralized validation with the override error code. */
  private static void validateOverride(
      RerankingProvidersConfig config, String provider, String modelName) {
    CollectionRerankDef.validateServiceDesc(
        config, provider, modelName, null, null, RequestException.Code.INVALID_RERANK_OVERRIDE);
  }

  @Test
  void resolvesAsymmetricHybridLimitsFromInnerCommands() throws Exception {
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

    var candidateResolver = mock(FindCommandResolver.class);
    Operation<CollectionSchemaObject> emptyRead = mock();
    var emptyResult = CommandResult.multiDocumentBuilder(RequestTracing.NO_OP).build();
    when(emptyRead.execute(commandContext))
        .thenReturn(Uni.createFrom().item(() -> () -> emptyResult));
    when(candidateResolver.resolveCollectionCandidateCommand(eq(commandContext), any()))
        .thenReturn(emptyRead);

    var operation =
        new FindAndRerankOperationBuilder(commandContext)
            .withCommand(command)
            .withFindCommandResolver(candidateResolver)
            .build();

    var result = operation.execute(commandContext).await().indefinitely().get();
    assertThat(result.errors()).isEmpty();

    var commands = ArgumentCaptor.forClass(FindCommand.class);
    verify(candidateResolver, times(2))
        .resolveCollectionCandidateCommand(eq(commandContext), commands.capture());
    verify(candidateResolver, never()).resolveCommand(any(), any());

    var reads =
        commands.getAllValues().stream()
            .map(
                inner ->
                    findCommandResolver.resolveCollectionCandidateCommand(commandContext, inner))
            .map(FindCollectionOperation.class::cast)
            .toList();
    assertThat(reads)
        .filteredOn(read -> read.vector() != null)
        .singleElement()
        .satisfies(
            read -> {
              assertThat(read.limit()).isEqualTo(RerankingConstants.HybridSearchLimits.MAX);
              assertThat(read.pageSize()).isEqualTo(RerankingConstants.HybridSearchLimits.MAX);
            });
    assertThat(reads)
        .filteredOn(read -> read.vector() == null)
        .singleElement()
        .satisfies(
            read -> {
              assertThat(read.limit()).isEqualTo(25);
              assertThat(read.pageSize()).isEqualTo(25);
            });
  }

  @Test
  void failsWhenVectorLimitAboveConfiguredMax() throws Exception {
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
                  "hybridLimits": { "$vector": 101, "$lexical": 50 }
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$vector")
        .hasMessageContaining("101")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  void failsWhenLexicalLimitAboveConfiguredMax() throws Exception {
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
                  "hybridLimits": { "$vector": 50, "$lexical": 101 }
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$lexical")
        .hasMessageContaining("101")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  public void failsWhenExplicitLexicalSortWhenLexicalDisabled() throws Exception {
    var commandContext = commandContext(false);
    var command =
        command(
            """
                    {
                      "findAndRerank": {
                        "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                        "options": {
                          "rerankOn": "body",
                          "rerankQuery": "text",
                          "hybridLimits": { "$vector": 50, "$lexical": 10 }
                        }
                      }
                    }
                    """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining(
            "The collection without a lexical index: %s.%s."
                .formatted(TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME));
  }

  @Test
  public void acceptsImplicitLexicalWhenLexcialDisabled() throws Exception {
    var commandContext = commandContext(false);
    var command =
        command(
            """
                    {
                      "findAndRerank": {
                        "sort": { "$hybrid": "cheese" },
                        "options": {
                          "rerankOn": "body",
                          "rerankQuery": "text",
                          "hybridLimits": { "$vector": 50, "$lexical": 10 }
                        }
                      }
                    }
                    """);

    var operation =
        new FindAndRerankOperationBuilder(commandContext)
            .withCommand(command)
            .withFindCommandResolver(findCommandResolver)
            .build();
  }

  @Test
  void failsWhenLimitBelowConfiguredMin() throws Exception {
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
                  "hybridLimits": 0
                }
              }
            }
            """);

    assertThatThrownBy(
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build())
        .isInstanceOf(RequestException.class)
        .hasMessageContaining("hybridLimits.$vector")
        .hasMessageContaining("must be between 1 and 100");
  }

  @Test
  void acceptsBoundaryValues() throws Exception {
    var commandContextLow = commandContext();
    var commandLow =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": 1
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContextLow)
        .withCommand(commandLow)
        .withFindCommandResolver(findCommandResolver)
        .build();

    var commandContextHigh = commandContext();
    var commandHigh =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": 100
                }
              }
            }
            """);

    new FindAndRerankOperationBuilder(commandContextHigh)
        .withCommand(commandHigh)
        .withFindCommandResolver(findCommandResolver)
        .build();
  }

  @Test
  void failsWhenMissingRerankOnAndNotVectorizeSort() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankQuery": "text"
                }
              }
            }
            """);

    assertBuildFails(
        "failsWhenMissingRerankOnAndNotVectorizeSort()",
        commandContext,
        command,
        RequestException.class,
        RequestException.Code.MISSING_RERANK_ON);
  }

  @Test
  void failsWhenBlankRerankOnAndNotVectorizeSort() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vector": [0.1, 0.2, 0.3], "$lexical": "text" } },
                "options": {
                  "rerankOn": "   ",
                  "rerankQuery": "text"
                }
              }
            }
            """);

    assertBuildFails(
        "failsWhenBlankRerankOnAndNotVectorizeSort()",
        commandContext,
        command,
        RequestException.class,
        RequestException.Code.MISSING_RERANK_ON);
  }

  @Test
  void failsWhenHybridSortHasOnlyLexical() throws Exception {
    // COLLECTION_SCHEMA_OBJECT has a lexical index and reranking, but no vector
    var schemaObject = TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT;
    assertThat(schemaObject.vectorConfig().vectorEnabled()).isFalse();
    assertThat(schemaObject.lexicalDef().enabled()).isTrue();

    var commandContext = commandContext(schemaObject);
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$lexical": "text" } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text",
                  "hybridLimits": { "$vector": 30, "$lexical": 20 }
                }
              }
            }
            """);

    assertBuildFails(
        "failsWhenHybridSortHasOnlyLexical()",
        commandContext,
        command,
        SortException.class,
        SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT);
  }

  @Test
  void buildsOneReadPerLegInPlanOrder() throws Exception {
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
                  "includeScores": true,
                  "includeSortVector": true,
                  "hybridLimits": { "$vector": 40, "$lexical": 15 }
                }
              }
            }
            """);

    var operation =
        new FindAndRerankOperationBuilder(commandContext)
            .withCommand(command)
            .withFindCommandResolver(findCommandResolver)
            .build();

    // read task position = leg index: the lexical read is 0, the vector read is 1. Both reads get
    // includeScores as includeSimilarity, as before the planner, and each read has the limit of its
    // leg.
    assertThat(readTaskSnapshots(operation))
        .containsExactly(
            new ReadTaskSnapshot(
                0, List.of("$lexical"), 15, true, true, false, Rank.RankSource.BM25),
            new ReadTaskSnapshot(
                1, List.of("$vector"), 40, true, true, false, Rank.RankSource.VECTOR));
  }

  @Test
  void buildsEmbeddingGroupAndPairsDeferredsForVectorizeAndLexical() throws Exception {
    var commandContext =
        commandContext(TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT);
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$vectorize": "cheese", "$lexical": "cows" } }
              }
            }
            """);

    var operation =
        new FindAndRerankOperationBuilder(commandContext)
            .withCommand(command)
            .withFindCommandResolver(findCommandResolver)
            .build();

    var groups = innerTaskGroups(operation);
    assertThat(groups).as("embedding group, read group and rerank group").hasSize(3);
    // one vectorize leg, so one group key and one embedding task
    assertThat(groups.getFirst())
        .as("the first group only embeds")
        .hasSize(1)
        .allSatisfy(task -> assertThat(task).isInstanceOf(EmbeddingTask.class));

    // the embedding task must complete the deferred vectorize of the read that waits for it
    var vectorizeRead = (IntermediateCollectionReadTask) groups.get(1).get(1);
    var deferredVectorize = (DeferredVectorize) privateField(vectorizeRead, "deferredVectorize");
    assertThat((List<?>) privateField(groups.getFirst().getFirst(), "embeddingActions"))
        .as("embedding task embeds for the vectorize read")
        .singleElement()
        .isSameAs(deferredVectorize.deferredAction());

    // the vectorize read has no sort until the embedding is ready
    var defaultLimits = FindAndRerankCommand.HybridLimits.DEFAULT;
    assertThat(readTaskSnapshots(operation))
        .containsExactly(
            new ReadTaskSnapshot(
                0,
                List.of("$lexical"),
                defaultLimits.lexicalLimit(),
                false,
                false,
                false,
                Rank.RankSource.BM25),
            new ReadTaskSnapshot(
                1,
                List.of(),
                defaultLimits.vectorLimit(),
                false,
                false,
                true,
                Rank.RankSource.VECTOR));
  }

  @Test
  void failsWhenHybridSortHasNoValue() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": { "$hybrid": { "$lexical": null } },
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text"
                }
              }
            }
            """);

    assertBuildFails(
        "failsWhenHybridSortHasNoValue()",
        commandContext,
        command,
        SortException.class,
        SortException.Code.MISSING_HYBRID_SORT_VALUE);
  }

  @Test
  void failsWhenSortIsNotHybrid() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {
              "findAndRerank": {
                "sort": {},
                "options": {
                  "rerankOn": "body",
                  "rerankQuery": "text"
                }
              }
            }
            """);

    assertBuildFails(
        "failsWhenSortIsNotHybrid()",
        commandContext,
        command,
        SortException.class,
        SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT);
  }

  private <T extends APIException> void assertBuildFails(
      String context,
      CommandContext<CollectionSchemaObject> commandContext,
      FindAndRerankCommand command,
      Class<T> exceptionClass,
      ErrorCode<T> code) {

    var ex =
        assertThrowsExactly(
            exceptionClass,
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build(),
            context);

    assertThat(ex.code).as(context).isEqualTo(code.name());
  }

  private FindAndRerankCommand command(String json) throws Exception {
    return objectMapper.readValue(json, FindAndRerankCommand.class);
  }

  private CommandContext<CollectionSchemaObject> commandContext() {
    return commandContext(true);
  }

  private CommandContext<CollectionSchemaObject> commandContext(boolean withLexical) {
    return commandContext(
        withLexical
            ? TEST_CONSTANTS.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT
            : TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT);
  }

  private CommandContext<CollectionSchemaObject> commandContext(
      CollectionSchemaObject schemaObject) {

    var commandContext =
        TEST_CONSTANTS.collectionContext(CommandName.FIND_AND_RERANK, schemaObject);

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

    if (schemaObject.vectorConfig().getFirstVectorColumnWithVectorizeDefinition().isPresent()) {
      var embeddingProvider = mock(EmbeddingProvider.class);

      when(commandContext
              .embeddingProviderFactory()
              .create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
          .thenReturn(embeddingProvider);
    }

    return commandContext;
  }

  @Nested
  class ValidateRerankOverride {

    // Shared config: nvidia provider enabled with a single supported model
    private final RerankingProvidersConfig NVIDIA_SUPPORTED =
        rerankingProvidersConfig(
            "nvidia",
            true,
            rerankingModelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED));

    @Test
    void shouldAcceptSupportedProviderAndModel() {
      assertThatCode(() -> validateOverride(NVIDIA_SUPPORTED, "nvidia", "nvidia/rerank-v1"))
          .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectUnknownProvider() {
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "unknown-provider", "some-model"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("unknown-provider");
    }

    @Test
    void shouldRejectDisabledProvider() {
      var disabledConfig =
          rerankingProvidersConfig(
              "nvidia",
              false,
              rerankingModelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED));

      assertThatThrownBy(() -> validateOverride(disabledConfig, "nvidia", "nvidia/rerank-v1"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }

    @Test
    void shouldRejectNullModelName() {
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("Model name is required");
    }

    @Test
    void shouldRejectUnknownModel() {
      assertThatThrownBy(
              () -> validateOverride(NVIDIA_SUPPORTED, "nvidia", "nvidia/nonexistent-model"))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("nonexistent-model");
    }

    @Test
    void shouldRejectDeprecatedModel() {
      var config =
          rerankingProvidersConfig(
              "nvidia",
              true,
              rerankingModelConfig("nvidia/old-model", ApiModelSupport.SupportStatus.DEPRECATED));

      assertThatThrownBy(() -> validateOverride(config, "nvidia", "nvidia/old-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.DEPRECATED_AI_MODEL.name());
    }

    @Test
    void shouldRejectEndOfLifeModel() {
      var config =
          rerankingProvidersConfig(
              "nvidia",
              true,
              rerankingModelConfig("nvidia/eol-model", ApiModelSupport.SupportStatus.END_OF_LIFE));

      assertThatThrownBy(() -> validateOverride(config, "nvidia", "nvidia/eol-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.END_OF_LIFE_AI_MODEL.name());
    }

    @Test
    void shouldRejectUnknownProviderBeforeCheckingModelName() {
      // When both provider is unknown AND modelName is null, the provider check should
      // come first — user gets the more actionable "provider not supported" error
      assertThatThrownBy(() -> validateOverride(NVIDIA_SUPPORTED, "unknown-provider", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("unknown-provider")
          .hasMessageContaining("not supported");
    }

    @Test
    void shouldRejectDisabledProviderBeforeCheckingModelName() {
      // When provider is disabled AND modelName is null, the disabled check should
      // come first — user gets "provider disabled" instead of "modelName required"
      var disabledConfig =
          rerankingProvidersConfig(
              "nvidia",
              false,
              rerankingModelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED));

      assertThatThrownBy(() -> validateOverride(disabledConfig, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }
  }
}
