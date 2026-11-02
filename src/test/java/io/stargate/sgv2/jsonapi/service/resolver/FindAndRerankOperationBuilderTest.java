package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link FindAndRerankOperationBuilder}: the operation it builds from a {@link
 * FindAndRerankPlan} the test makes. The checks that make the plan are tested in {@link
 * FindAndRerankPlannerTest}.
 *
 * <p>The operation does not expose its tasks, so the tests check the providers the builder creates,
 * the hybridLimits it sets on the command context, and the inner finds the operation runs when the
 * test runs it with reads that return no documents.
 */
class FindAndRerankOperationBuilderTest {

  /** The vector of the plans that read with the vector from the command. */
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

  /** The vector {@link TestEmbeddingProvider} returns for the text of {@link #VECTORIZE_STEP}. */
  private static final float[] VECTORIZED = {0.25f, 0.25f, 0.25f};

  /** The text the lexical legs sort with. */
  private static final String LEXICAL_TEXT = "cows";

  /** The vectorize step of the plans that vectorize. */
  private static final FindAndRerankPlan.VectorizeStep VECTORIZE_STEP =
      new FindAndRerankPlan.VectorizeStep(
          "cheese", VECTOR.length, new VectorizeDefinition("custom", "custom", null, null));

  private static final CollectionRerankDef.RerankServiceDef RERANK_SERVICE =
      new CollectionRerankDef.RerankServiceDef("nvidia", TestConstants.RERANK_MODEL, null, null);

  private static final FindAndRerankPlan.RerankStep RERANK_STEP =
      new FindAndRerankPlan.RerankStep(
          RERANK_SERVICE,
          mock(RerankingQuery.class),
          PathMatchLocator.forPath("body"),
          DocumentProjector.defaultProjector(),
          10);

  /** The builder only reads the filter and the hybridLimits of the command. */
  private static final String EMPTY_COMMAND =
      """
      {"findAndRerank": {}}
      """;

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  // the read tasks only keep the resolver, building the operation does not call it
  private final FindCommandResolver findCommandResolver = mock(FindCommandResolver.class);

  // ===========================================================================================
  // Providers
  // ===========================================================================================

  @Test
  void createsRerankingProviderFromRerankStep() throws Exception {
    var commandContext = commandContext();

    build(commandContext, plan(null, vectorLeg(50)));

    verify(commandContext.rerankingProviderFactory())
        .create(
            any(),
            any(),
            eq(RERANK_SERVICE.provider()),
            eq(RERANK_SERVICE.modelName()),
            any(),
            any());
  }

  /** A plan that reads with the vector from the command has nothing to vectorize. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("plansWithoutVectorize")
  void createsNoEmbeddingProviderWithoutVectorizeStep(String description, FindAndRerankPlan plan)
      throws Exception {
    var commandContext = commandContext();

    build(commandContext, plan);

    verifyNoInteractions(commandContext.embeddingProviderFactory());
  }

  static Stream<Arguments> plansWithoutVectorize() {
    return Stream.of(
        Arguments.of("vector leg", plan(null, vectorLeg(50))),
        Arguments.of("vector and lexical legs", plan(null, vectorLeg(50), lexicalLeg(25))));
  }

  /** The embedding provider for the vectorize step uses the service and dimension of the step. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("plansWithVectorize")
  void createsEmbeddingProviderForVectorizeStep(String description, FindAndRerankPlan plan)
      throws Exception {
    var commandContext = commandContext();
    when(commandContext
            .embeddingProviderFactory()
            .create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(mock(EmbeddingProvider.class));

    build(commandContext, plan);

    verify(commandContext.embeddingProviderFactory(), times(1).description(description))
        .create(
            any(),
            any(),
            eq(VECTORIZE_STEP.vectorizeDefinition().provider()),
            eq(VECTORIZE_STEP.vectorizeDefinition().modelName()),
            eq(VECTORIZE_STEP.dimension()),
            any(),
            any(),
            any());
  }

  static Stream<Arguments> plansWithVectorize() {
    return Stream.of(
        Arguments.of("vectorize, then the vector leg", plan(VECTORIZE_STEP, vectorizedLeg(50))),
        Arguments.of(
            "vectorize, then the vector and lexical legs",
            plan(VECTORIZE_STEP, vectorizedLeg(50), lexicalLeg(25))));
  }

  // ===========================================================================================
  // Reads
  // ===========================================================================================

  /**
   * The operation runs one inner find for each leg of the plan, with the sort and the read limit of
   * the leg. The reads run in parallel, so the test does not check their order.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("plansAndReads")
  void runsOneReadPerLeg(String description, FindAndRerankPlan plan, List<InnerRead> expectedReads)
      throws Exception {
    var reads = runAndCaptureReads(commandContext(), plan);

    assertThat(reads).as(description).containsExactlyInAnyOrderElementsOf(expectedReads);
  }

  static Stream<Arguments> plansAndReads() {
    return Stream.of(
        Arguments.of("vector leg", plan(null, vectorLeg(50)), List.of(vectorRead(VECTOR, 50))),
        Arguments.of(
            "vector and lexical legs",
            plan(null, vectorLeg(100), lexicalLeg(25)),
            List.of(vectorRead(VECTOR, 100), lexicalRead(25))),
        Arguments.of(
            "vectorize, then the vector leg",
            plan(VECTORIZE_STEP, vectorizedLeg(50)),
            List.of(vectorRead(VECTORIZED, 50))),
        Arguments.of(
            "vectorize, then the vector and lexical legs",
            plan(VECTORIZE_STEP, vectorizedLeg(50), lexicalLeg(25)),
            List.of(vectorRead(VECTORIZED, 50), lexicalRead(25))),
        // the planner does not make this plan yet, a single $lexical sort will (GH #2602)
        Arguments.of("lexical leg", plan(null, lexicalLeg(25)), List.of(lexicalRead(25))));
  }

  /** The inner finds get includeScores as includeSimilarity, and get includeSortVector. */
  @Test
  void readsUseIncludeScoresAndIncludeSortVectorOfPlan() throws Exception {
    var plan =
        new FindAndRerankPlan(
            null, List.of(vectorLeg(100), lexicalLeg(25)), RERANK_STEP, true, true);

    var reads = runAndCaptureReads(commandContext(), plan);

    assertThat(reads)
        .containsExactlyInAnyOrder(
            new InnerRead(vectorSort(VECTOR), 100, true, true),
            new InnerRead(lexicalSort(), 25, true, true));
  }

  // ===========================================================================================
  // Plans the builder cannot build
  // ===========================================================================================

  /** Sanity check, the planner does not make a FILTER_LEG yet. */
  @Test
  void failsWhenPlanHasFilterLeg() {
    var plan =
        plan(null, new FindAndRerankPlan.RerankLeg(RerankLegType.FILTER_LEG, null, null, 20));

    assertThatThrownBy(() -> build(commandContext(), plan))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("rankSource() - the builder cannot read with a FILTER_LEG yet")
        .hasMessageContaining("legTypes=[FILTER_LEG]");
  }

  // ===========================================================================================
  // Command context
  // ===========================================================================================

  @Test
  void keepsExplicitHybridLimitsAtMaximumPageSizeOnCommandContext() throws Exception {
    var commandContext = commandContext();
    var command =
        command(
            """
            {"findAndRerank": {"options": {"hybridLimits": {"$vector": 100, "$lexical": 25}}}}
            """);

    build(commandContext, command, plan(null, vectorLeg(100), lexicalLeg(25)));

    assertThat(commandContext.getHybridLimits().vectorLimit())
        .isEqualTo(RerankingConstants.HybridSearchLimits.MAX);
    assertThat(commandContext.getHybridLimits().lexicalLimit()).isEqualTo(25);
  }

  // ===========================================================================================
  // Helpers / no more tests below
  // ===========================================================================================

  private Operation<CollectionSchemaObject> build(
      CommandContext<CollectionSchemaObject> commandContext, FindAndRerankPlan plan)
      throws Exception {
    return build(commandContext, command(EMPTY_COMMAND), plan);
  }

  /** Builds the operation the way {@link FindAndRerankCommandResolver} does, from the plan. */
  private Operation<CollectionSchemaObject> build(
      CommandContext<CollectionSchemaObject> commandContext,
      FindAndRerankCommand command,
      FindAndRerankPlan plan) {
    return new FindAndRerankOperationBuilder(commandContext)
        .withCommand(command)
        .withPlan(plan)
        .withFindCommandResolver(findCommandResolver)
        .build();
  }

  /**
   * Builds the operation and runs it. Every inner find returns no documents, so the reranking task
   * does not call the reranking provider.
   *
   * @return The inner finds the operation ran, one for each read.
   */
  private List<InnerRead> runAndCaptureReads(
      CommandContext<CollectionSchemaObject> commandContext, FindAndRerankPlan plan)
      throws Exception {

    when(commandContext
            .embeddingProviderFactory()
            .create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(TestEmbeddingProvider.TEST_EMBEDDING_PROVIDER);

    var noDocuments = CommandResult.multiDocumentBuilder(RequestTracing.NO_OP).build();
    Operation<CollectionSchemaObject> emptyRead = mock();
    when(emptyRead.execute(commandContext))
        .thenReturn(Uni.createFrom().item(() -> () -> noDocuments));
    when(findCommandResolver.resolveCommand(eq(commandContext), any())).thenReturn(emptyRead);

    var result = build(commandContext, plan).execute(commandContext).await().indefinitely().get();
    // the reranking task fails if a read did not fill the deferred result the task waits for
    assertThat(result.errors()).isEmpty();

    var findCommands = ArgumentCaptor.forClass(FindCommand.class);
    verify(findCommandResolver, times(plan.legs().size()))
        .resolveCommand(eq(commandContext), findCommands.capture());
    return findCommands.getAllValues().stream()
        .map(findCommand -> InnerRead.from(commandContext, findCommand))
        .toList();
  }

  private FindAndRerankCommand command(String json) throws Exception {
    return objectMapper.readValue(json, FindAndRerankCommand.class);
  }

  /** A context for a collection that can run every kind of sort, with a mock reranking provider. */
  private CommandContext<CollectionSchemaObject> commandContext() {
    var commandContext =
        TEST_CONSTANTS.collectionContext(
            CommandName.FIND_AND_RERANK,
            TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT);
    when(commandContext.rerankingProviderFactory().create(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(RerankingProvider.class));
    return commandContext;
  }

  private static FindAndRerankPlan plan(
      FindAndRerankPlan.VectorizeStep vectorizeStep, FindAndRerankPlan.RerankLeg... legs) {
    return new FindAndRerankPlan(vectorizeStep, List.of(legs), RERANK_STEP, false, false);
  }

  /** A VECTOR_LEG that reads with {@link #VECTOR}. */
  private static FindAndRerankPlan.RerankLeg vectorLeg(int readLimit) {
    return new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, readLimit);
  }

  /** A VECTOR_LEG that reads with the vector from the vectorize step. */
  private static FindAndRerankPlan.RerankLeg vectorizedLeg(int readLimit) {
    return new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, readLimit);
  }

  private static FindAndRerankPlan.RerankLeg lexicalLeg(int readLimit) {
    return new FindAndRerankPlan.RerankLeg(
        RerankLegType.LEXICAL_LEG, null, LEXICAL_TEXT, readLimit);
  }

  /**
   * An inner find the operation ran.
   *
   * @param sort The sort expressions of the find as <code>path=value</code> text, so that a vector
   *     compares by value.
   * @param limit The limit of the find.
   * @param includeSimilarity The <code>includeSimilarity</code> option of the find.
   * @param includeSortVector The <code>includeSortVector</code> option of the find.
   */
  private record InnerRead(
      String sort, int limit, boolean includeSimilarity, boolean includeSortVector) {

    static InnerRead from(
        CommandContext<CollectionSchemaObject> commandContext, FindCommand findCommand) {
      var sort =
          findCommand.sortClause(commandContext).sortExpressions().stream()
              .map(
                  expression ->
                      expression.getPath()
                          + "="
                          + (expression.hasVector()
                              ? Arrays.toString(expression.getVector())
                              : expression.getLexicalQuery()))
              .collect(Collectors.joining(", "));
      var options = findCommand.options();
      return new InnerRead(
          sort, options.limit(), options.includeSimilarity(), options.includeSortVector());
    }
  }

  /** The read for a VECTOR_LEG, without includeSimilarity and includeSortVector. */
  private static InnerRead vectorRead(float[] vector, int limit) {
    return new InnerRead(vectorSort(vector), limit, false, false);
  }

  /** The read for {@link #lexicalLeg(int)}, without includeSimilarity and includeSortVector. */
  private static InnerRead lexicalRead(int limit) {
    return new InnerRead(lexicalSort(), limit, false, false);
  }

  private static String vectorSort(float[] vector) {
    return VECTOR_EMBEDDING_FIELD + "=" + Arrays.toString(vector);
  }

  private static String lexicalSort() {
    return LEXICAL_CONTENT_FIELD + "=" + LEXICAL_TEXT;
  }

  /** Helper that calls the centralized validation with the override error code. */
  private static void validateOverride(
      RerankingProvidersConfig config, String provider, String modelName) {
    CollectionRerankDef.validateServiceDesc(
        config, provider, modelName, null, null, RequestException.Code.INVALID_RERANK_OVERRIDE);
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
