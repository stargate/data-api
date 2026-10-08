package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingTask;
import io.stargate.sgv2.jsonapi.service.operation.reranking.IntermediateCollectionReadTask;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingTask;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderTest {

  // @QuarkusTest needed for error template initialization
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  // Reusable request properties for model configs
  private static final RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
          .RequestPropertiesImpl
      REQUEST_PROPERTIES =
          new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
              .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10);

  private static RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig(
      String name, ApiModelSupport.SupportStatus status) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
        name,
        new ApiModelSupport.ApiModelSupportImpl(status, Optional.empty()),
        false,
        "https://example.com/rerank",
        REQUEST_PROPERTIES);
  }

  private static RerankingProvidersConfig configWithProvider(
      String providerName,
      boolean enabled,
      List<RerankingProvidersConfig.RerankingProviderConfig.ModelConfig> models) {
    return new RerankingProvidersConfigImpl(
        Map.of(
            providerName,
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
                false, providerName, enabled, Map.of(), models)));
  }

  /** Helper that calls the centralized validation with the override error code. */
  private static void validateOverride(
      RerankingProvidersConfig config, String provider, String modelName) {
    CollectionRerankDef.validateServiceDesc(
        config, provider, modelName, null, null, RequestException.Code.INVALID_RERANK_OVERRIDE);
  }

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
        .isEqualTo(RerankingConstants.HybridSearchLimits.MAX);
    assertThat(commandContext.getHybridLimits().lexicalLimit()).isEqualTo(25);
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

    assertMissingRerankOn("failsWhenMissingRerankOnAndNotVectorizeSort()", commandContext, command);
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

    assertMissingRerankOn("failsWhenBlankRerankOnAndNotVectorizeSort()", commandContext, command);
  }

  @Test
  void buildsOneLexicalReadForLexicalOnlyHybridWithoutVector() throws Exception {
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

    var operation =
        new FindAndRerankOperationBuilder(commandContext)
            .withCommand(command)
            .withFindCommandResolver(findCommandResolver)
            .build();

    assertThat(innerTaskGroups(operation))
        .as("read group and rerank group, no embedding group")
        .hasSize(2);
    assertThat(readTasks(operation))
        .as("one lexical read at position 0")
        .containsExactly(
            new ReadTaskDesc(
                0, List.of("$lexical"), 20, false, false, false, Rank.RankSource.BM25));
    assertThat(commandContext.getHybridLimits().lexicalLimit()).isEqualTo(20);
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

    // read task position = leg index: the lexical read is 0, the vector read is 1. Only the vector
    // read includes the similarity, and each read has the limit of its leg.
    assertThat(readTasks(operation))
        .containsExactly(
            new ReadTaskDesc(0, List.of("$lexical"), 15, false, true, false, Rank.RankSource.BM25),
            new ReadTaskDesc(1, List.of("$vector"), 40, true, true, false, Rank.RankSource.VECTOR));
  }

  @Test
  void buildsEmbeddingGroupAndPairsDeferredsForVectorizeAndLexical() throws Exception {
    var commandContext = commandContext(vectorizeLexicalRerankSchema());
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
    assertThat(groups.getFirst())
        .as("the first group only embeds")
        .isNotEmpty()
        .allSatisfy(task -> assertThat(task).isInstanceOf(EmbeddingTask.class));

    // the vectorize read has no sort until the embedding is ready
    var defaultLimits = FindAndRerankCommand.HybridLimits.DEFAULT;
    assertThat(readTasks(operation))
        .containsExactly(
            new ReadTaskDesc(
                0,
                List.of("$lexical"),
                defaultLimits.lexicalLimit(),
                false,
                false,
                false,
                Rank.RankSource.BM25),
            new ReadTaskDesc(
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
        RequestException.Code.MISSING_HYBRID_SORT_VALUE);
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
        RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT);
  }

  private void assertMissingRerankOn(
      String context,
      CommandContext<CollectionSchemaObject> commandContext,
      FindAndRerankCommand command) {

    var ex =
        assertThrowsExactly(
            RequestException.class,
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build(),
            context);

    assertThat(ex.code).as(context).isEqualTo(RequestException.Code.MISSING_RERANK_ON.name());
  }

  private void assertBuildFails(
      String context,
      CommandContext<CollectionSchemaObject> commandContext,
      FindAndRerankCommand command,
      RequestException.Code code) {

    var ex =
        assertThrowsExactly(
            RequestException.class,
            () ->
                new FindAndRerankOperationBuilder(commandContext)
                    .withCommand(command)
                    .withFindCommandResolver(findCommandResolver)
                    .build(),
            context);

    assertThat(ex.code).as(context).isEqualTo(code.name());
  }

  /**
   * What the builder put on a read task, at build time.
   *
   * @param position The position of the task.
   * @param sortPaths The sort paths of the inner find, empty for a vectorize read until the vector
   *     is ready.
   * @param limit The limit of the inner find.
   * @param includeSimilarity The <code>includeSimilarity</code> option of the inner find.
   * @param includeSortVector The <code>includeSortVector</code> option of the inner find.
   * @param hasDeferredVectorize True if the read waits for a vectorize.
   * @param rankSource The rank source of the deferred read the task fills.
   */
  private record ReadTaskDesc(
      int position,
      List<String> sortPaths,
      int limit,
      boolean includeSimilarity,
      boolean includeSortVector,
      boolean hasDeferredVectorize,
      Rank.RankSource rankSource) {}

  /**
   * The inner task groups of the operation, in the order they run. The task tree is private, so
   * this reads it with reflection: the operation runs one composite task per group.
   */
  private static List<List<?>> innerTaskGroups(Operation<CollectionSchemaObject> operation)
      throws ReflectiveOperationException {
    var groups = new ArrayList<List<?>>();
    for (var compositeTask : (List<?>) privateField(operation, "taskGroup")) {
      groups.add((List<?>) privateField(compositeTask, "innerTaskGroup"));
    }
    return groups;
  }

  /**
   * The read tasks of the operation, in the order they were added to their group. Also checks that
   * there is one read task per deferred read of the reranking task, and that the read task at
   * position i fills the deferred read at index i.
   */
  private static List<ReadTaskDesc> readTasks(Operation<CollectionSchemaObject> operation)
      throws ReflectiveOperationException {
    var deferredReads = rerankDeferredReads(operation);
    var readTasks = new ArrayList<ReadTaskDesc>();
    for (var group : innerTaskGroups(operation)) {
      for (var task : group) {
        if (task instanceof IntermediateCollectionReadTask readTask) {
          var deferredRead = deferredReads.get(readTask.position());
          assertThat(privateField(readTask, "commandResultAction"))
              .as("read task %s fills the deferred read at the same index", readTask.position())
              .isSameAs(deferredRead.deferredRead().deferredAction());

          var findCommand = (FindCommand) privateField(readTask, "findCommand");
          readTasks.add(
              new ReadTaskDesc(
                  readTask.position(),
                  findCommand.sortDefinition().getSortExpressionPaths(),
                  findCommand.options().limit(),
                  findCommand.options().includeSimilarity(),
                  findCommand.options().includeSortVector(),
                  privateField(readTask, "deferredVectorize") != null,
                  deferredRead.rankSource()));
        }
      }
    }
    assertThat(readTasks).as("one read task per deferred read").hasSameSizeAs(deferredReads);
    return readTasks;
  }

  /** The deferred reads of the reranking task, which is the only task in the last group. */
  @SuppressWarnings("unchecked")
  private static List<RerankingTask.DeferredCommandWithSource> rerankDeferredReads(
      Operation<CollectionSchemaObject> operation) throws ReflectiveOperationException {
    var lastGroup = innerTaskGroups(operation).getLast();
    assertThat(lastGroup).as("rerank group").hasSize(1);
    var rerankTask = lastGroup.getFirst();
    assertThat(rerankTask).isInstanceOf(RerankingTask.class);
    return (List<RerankingTask.DeferredCommandWithSource>)
        privateField(rerankTask, "deferredReads");
  }

  private static Object privateField(Object target, String name)
      throws ReflectiveOperationException {
    var field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
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

  /** TestConstants has no schema with both a vectorize service and a lexical index. */
  private CollectionSchemaObject vectorizeLexicalRerankSchema() {
    return new CollectionSchemaObject(
        TEST_CONSTANTS.COLLECTION_IDENTIFIER,
        IdConfig.defaultIdConfig(),
        VectorConfig.fromColumnDefinitions(
            List.of(
                new VectorColumnDefinition(
                    DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                    -1,
                    SimilarityFunction.COSINE,
                    EmbeddingSourceModel.OTHER,
                    new VectorizeDefinition("custom", "custom", null, null)))),
        null,
        CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(null),
        CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(
            new CollectionRerankDef(
                true,
                new CollectionRerankDef.RerankServiceDef(
                    "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null))));
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
        configWithProvider(
            "nvidia",
            true,
            List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

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
          configWithProvider(
              "nvidia",
              false,
              List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

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
          configWithProvider(
              "nvidia",
              true,
              List.of(modelConfig("nvidia/old-model", ApiModelSupport.SupportStatus.DEPRECATED)));

      assertThatThrownBy(() -> validateOverride(config, "nvidia", "nvidia/old-model"))
          .isInstanceOf(SchemaException.class)
          .hasFieldOrPropertyWithValue("code", SchemaException.Code.DEPRECATED_AI_MODEL.name());
    }

    @Test
    void shouldRejectEndOfLifeModel() {
      var config =
          configWithProvider(
              "nvidia",
              true,
              List.of(modelConfig("nvidia/eol-model", ApiModelSupport.SupportStatus.END_OF_LIFE)));

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
          configWithProvider(
              "nvidia",
              false,
              List.of(modelConfig("nvidia/rerank-v1", ApiModelSupport.SupportStatus.SUPPORTED)));

      assertThatThrownBy(() -> validateOverride(disabledConfig, "nvidia", null))
          .isInstanceOf(RequestException.class)
          .hasFieldOrPropertyWithValue("code", RequestException.Code.INVALID_RERANK_OVERRIDE.name())
          .hasMessageContaining("disabled");
    }
  }
}
