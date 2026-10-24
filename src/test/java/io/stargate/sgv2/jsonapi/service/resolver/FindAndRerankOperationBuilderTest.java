package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
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
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankOperationBuilder}: the operation it builds from a {@link
 * FindAndRerankPlan} the test makes. The checks that make the plan are tested in {@link
 * FindAndRerankPlannerTest}.
 *
 * <p>The operation does not expose its tasks, so the tests check the providers the builder creates
 * and the hybridLimits it sets on the command context.
 */
class FindAndRerankOperationBuilderTest {

  /** The vector of the plans that read with the vector from the command. */
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

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
  // Plans the builder cannot build
  // ===========================================================================================

  /** Sanity check, the planner always makes a VECTOR_LEG. */
  @Test
  void failsWhenPlanHasNoVectorLeg() {
    var plan = plan(null, lexicalLeg(25));

    assertThatThrownBy(() -> build(commandContext(), plan))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("buildVectorRead() - plan has no VECTOR_LEG")
        .hasMessageContaining("LEXICAL_LEG");
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

  private void build(CommandContext<CollectionSchemaObject> commandContext, FindAndRerankPlan plan)
      throws Exception {
    build(commandContext, command(EMPTY_COMMAND), plan);
  }

  /** Builds the operation the way {@link FindAndRerankCommandResolver} does, from the plan. */
  private void build(
      CommandContext<CollectionSchemaObject> commandContext,
      FindAndRerankCommand command,
      FindAndRerankPlan plan) {
    new FindAndRerankOperationBuilder(commandContext)
        .withCommand(command)
        .withPlan(plan)
        .withFindCommandResolver(findCommandResolver)
        .build();
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
      FindAndRerankPlan.VectorizeStep vectorize, FindAndRerankPlan.RerankLeg... legs) {
    return new FindAndRerankPlan(vectorize, List.of(legs), RERANK_STEP, false, false);
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
    return new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, "cows", readLimit);
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
