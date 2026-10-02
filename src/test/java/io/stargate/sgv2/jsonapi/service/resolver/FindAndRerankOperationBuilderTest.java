package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.LEXICAL_CONTENT_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.TestEmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.reranking.DocumentScoresDesc;
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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

/**
 * Tests for {@link FindAndRerankOperationBuilder}: the operation it builds from a {@link
 * FindAndRerankPlan} the test makes. The checks that make the plan are tested in {@link
 * FindAndRerankPlannerTest}.
 *
 * <p>The operation does not expose its tasks, so the tests check the providers the builder creates,
 * and what happens when the test runs the operation with mock reads: the inner finds it runs,
 * and the ranks the reranking task gives the documents each read finds.
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

  /** The builder only reads the filter of the command. */
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

  /**
   * The inner finds get includeScores as includeSimilarity, and get includeSortVector. The two
   * options have different values, so the test fails if the builder swaps them.
   */
  @ParameterizedTest(name = "includeScores={0}, includeSortVector={1}")
  @CsvSource({"true, false", "false, true"})
  void readsUseIncludeScoresAndIncludeSortVectorOfPlan(
      boolean includeScores, boolean includeSortVector) throws Exception {
    var plan =
        new FindAndRerankPlan(
            null,
            List.of(vectorLeg(100), lexicalLeg(25)),
            RERANK_STEP,
            includeScores,
            includeSortVector);

    var reads = runAndCaptureReads(commandContext(), plan);

    assertThat(reads)
        .containsExactlyInAnyOrder(
            new InnerRead(vectorSort(VECTOR), 100, includeScores, includeSortVector),
            new InnerRead(lexicalSort(), 25, includeScores, includeSortVector));
  }

  /**
   * The reranking task ranks each document by the leg that read it: the document from the vector
   * read gets a vector rank and no BM25 rank, and the document from the lexical read gets the
   * opposite.
   */
  @Test
  void ranksEachDocumentByTheLegThatReadIt() throws Exception {
    var commandContext =
        TEST_CONSTANTS.collectionContext(
            CommandName.FIND_AND_RERANK,
            TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT,
            new SimpleMeterRegistry());
    var rerankingProvider = sameScoreRerankingProvider();
    when(commandContext.rerankingProviderFactory().create(any(), any(), any(), any(), any(), any()))
        .thenReturn(rerankingProvider);

    // each read finds a different document, so the ranks of a document show which read found it
    var vectorRead =
        readFinding(
            commandContext,
            """
            {"_id": "from-vector", "body": "vector passage", "$similarity": 0.9}
            """);
    var lexicalRead =
        readFinding(
            commandContext,
            """
            {"_id": "from-lexical", "body": "lexical passage"}
            """);
    when(findCommandResolver.resolveCollectionCandidateCommand(eq(commandContext), any()))
        .thenAnswer(
            invocation -> {
              FindCommand findCommand = invocation.getArgument(1);
              var sortExpression =
                  findCommand.sortClause(commandContext).sortExpressions().getFirst();
              return sortExpression.hasVector() ? vectorRead : lexicalRead;
            });

    // includeScores, so the response has the ranks of each document
    var plan =
        new FindAndRerankPlan(
            null, List.of(vectorLeg(50), lexicalLeg(25)), RERANK_STEP, true, false);
    var result = build(commandContext, plan).execute(commandContext).await().indefinitely().get();

    assertThat(result.errors()).isEmpty();
    assertThat(ranksById(result))
        .isEqualTo(Map.of("from-vector", new Ranks(1, null), "from-lexical", new Ranks(null, 1)));
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

    var emptyRead = readFinding(commandContext);
    when(findCommandResolver.resolveCollectionCandidateCommand(eq(commandContext), any())).thenReturn(emptyRead);

    var result = build(commandContext, plan).execute(commandContext).await().indefinitely().get();
    // the reranking task fails if a read did not fill the deferred result the task waits for
    assertThat(result.errors()).isEmpty();

    var findCommands = ArgumentCaptor.forClass(FindCommand.class);
    verify(findCommandResolver, times(plan.legs().size()))
        .resolveCollectionCandidateCommand(eq(commandContext), findCommands.capture());
    verify(findCommandResolver, never()).resolveCommand(any(), any());
    return findCommands.getAllValues().stream()
        .map(findCommand -> InnerRead.from(commandContext, findCommand))
        .toList();
  }

  /** An inner find that finds only these documents, no documents when there are none. */
  private Operation<CollectionSchemaObject> readFinding(
      CommandContext<CollectionSchemaObject> commandContext, String... documentsJson)
      throws Exception {
    var resultBuilder = CommandResult.multiDocumentBuilder(RequestTracing.NO_OP);
    for (var documentJson : documentsJson) {
      resultBuilder.addDocument(objectMapper.readTree(documentJson));
    }
    var found = resultBuilder.build();
    Operation<CollectionSchemaObject> read = mock();
    when(read.execute(commandContext)).thenReturn(Uni.createFrom().item(() -> () -> found));
    return read;
  }

  /**
   * A reranking provider that gives every passage the same score, so the ranks from the reads are
   * the only scores that differ between documents.
   */
  private static RerankingProvider sameScoreRerankingProvider() {
    var rerankingProvider = mock(RerankingProvider.class);
    // the reranking metrics are tagged with the model name
    when(rerankingProvider.modelName()).thenReturn("test-model");
    when(rerankingProvider.rerank(any(), anyList(), any()))
        .thenAnswer(
            invocation -> {
              List<String> passages = invocation.getArgument(1);
              var ranks =
                  IntStream.range(0, passages.size())
                      .mapToObj(index -> new RerankingProvider.Rank(index, 0.5f))
                      .toList();
              // no model usage, so there is no billing event
              return Uni.createFrom().item(new RerankingProvider.RerankingResponse(ranks, null));
            });
    return rerankingProvider;
  }

  /**
   * The ranks of a document in the response.
   *
   * @param vectorRank The rank from the vector read, <code>null</code> when it did not find the
   *     document.
   * @param bm25Rank The rank from the lexical read, <code>null</code> when it did not find the
   *     document.
   */
  private record Ranks(Integer vectorRank, Integer bm25Rank) {}

  /** The ranks in the response, by the _id of their document. */
  @SuppressWarnings("unchecked")
  private static Map<String, Ranks> ranksById(CommandResult result) {
    var documents = ((ResponseData.MultiResponseData) result.data()).documents();
    var scores = (List<DocumentScoresDesc>) result.status().get(CommandStatus.DOCUMENT_RESPONSES);
    // the response lists the scores in the same order as the documents
    assertThat(scores).hasSameSizeAs(documents);
    return IntStream.range(0, documents.size())
        .boxed()
        .collect(
            Collectors.toMap(
                index -> documents.get(index).get(DOC_ID).asText(),
                index ->
                    new Ranks(
                        scores.get(index).scores().vectorRank(),
                        scores.get(index).scores().bm25Rank())));
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
