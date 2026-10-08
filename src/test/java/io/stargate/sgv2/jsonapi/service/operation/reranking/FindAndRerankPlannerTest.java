package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.LegMode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.config.IntConfigWithBounds;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankPlanner}, plain JUnit without Quarkus.
 *
 * <p>The mode and the explicit <code>$lexical</code> flag are passed in, the sorts are built with
 * the values the parser gives for each request, the JSON of the request is in the comments.
 */
public class FindAndRerankPlannerTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String RERANK_MODEL = "nvidia/llama-3.2-nv-rerankqa-1b-v2";
  private static final String OTHER_RERANK_MODEL = "nvidia/other-rerank-model";
  private static final CollectionRerankDef.RerankServiceDef COLLECTION_RERANK_SERVICE =
      new CollectionRerankDef.RerankServiceDef("nvidia", RERANK_MODEL, null, null);
  private static final VectorizeDefinition VECTORIZE_DEF =
      new VectorizeDefinition("custom", "custom", null, null);
  private static final int DIMENSION = 3;
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

  // config values that differ from the production defaults, to show where each value comes from
  private static final int CONFIG_LIMIT = 7;
  private static final OperationsConfig OPERATIONS_CONFIG = operationsConfig();
  private static final RerankingProvidersConfig PROVIDERS_CONFIG =
      providersConfig(ApiModelSupport.SupportStatus.SUPPORTED);

  // read limit when the command has no hybridLimits, never the config default of 20
  private static final int DEFAULT_READ = FindAndRerankCommand.HybridLimits.DEFAULT.vectorLimit();

  private static final CollectionSchemaObject VECTORIZE_LEXICAL =
      collection(true, true, true, true);
  private static final CollectionSchemaObject VECTORIZE_NO_LEXICAL =
      collection(true, true, false, true);
  private static final CollectionSchemaObject VECTOR_LEXICAL = collection(true, false, true, true);
  private static final CollectionSchemaObject LEXICAL_NO_VECTOR =
      collection(false, false, true, true);
  private static final CollectionSchemaObject VECTOR_NO_RERANK =
      collection(true, false, false, false);
  private static final CollectionSchemaObject NOTHING_NO_RERANK =
      collection(false, false, false, false);

  private static final String NO_OPTIONS = null;
  private static final String RERANK_OPTIONS = "{'rerankQuery': 'q', 'rerankOn': 'body'}";

  private static final LexicalLeg LEXICAL_COWS = new LexicalLeg("cows", DEFAULT_READ);
  private static final VectorizeLeg VECTORIZE_CHEESE =
      new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, DEFAULT_READ);
  private static final VectorLeg VECTOR_LEG = new VectorLeg(VECTOR, DEFAULT_READ);

  @Nested
  class HybridLegs {

    @Test
    void hybridTextWithLexicalIndex() {
      // {"$hybrid": "cheese"}
      var plan = plan(LegMode.HYBRID, false, sort("cheese", "cheese", null), VECTORIZE_LEXICAL);

      assertThat(plan.legs())
          .as("lexical leg first, read limits from HybridLimits.DEFAULT")
          .containsExactly(new LexicalLeg("cheese", DEFAULT_READ), VECTORIZE_CHEESE);
      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
      assertPassage(plan, DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      assertThat(plan.limit()).as("limit from config").isEqualTo(CONFIG_LIMIT);
      assertThat(plan.rerankServiceDef())
          .as("collection rerank service")
          .isEqualTo(COLLECTION_RERANK_SERVICE);
    }

    @Test
    void hybridTextWithoutLexicalIndexSkipsLexicalLeg() {
      // {"$hybrid": "cheese"}
      var plan = plan(LegMode.HYBRID, false, sort("cheese", "cheese", null), VECTORIZE_NO_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void vectorizeOnly() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan = plan(LegMode.HYBRID, false, sort("cheese", null, null), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void vectorOnly() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(LegMode.HYBRID, false, sort(null, null, VECTOR), RERANK_OPTIONS, VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTOR_LEG);
      assertQuery(plan, "q", RerankingQuery.Source.OPTIONS);
      assertPassage(plan, "body");
    }

    @Test
    void vectorizeWinsOverVector() {
      // {"$hybrid": {"$vectorize": "cheese", "$vector": [0.1, 0.2, 0.3]}}
      var plan = plan(LegMode.HYBRID, false, sort("cheese", null, VECTOR), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void vectorizeAndLexical() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      var plan = plan(LegMode.HYBRID, true, sort("cheese", "cows", null), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS, VECTORIZE_CHEESE);
      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
    }

    @Test
    void vectorAndLexical() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan =
          plan(LegMode.HYBRID, true, sort(null, "cows", VECTOR), RERANK_OPTIONS, VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS, VECTOR_LEG);
    }

    @Test
    void lexicalOnlyReadsLexicalHybridLimit() {
      // {"$hybrid": {"$lexical": "cows"}}
      var plan =
          plan(
              LegMode.HYBRID,
              true,
              sort(null, "cows", null),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 30, '$lexical': 15}}",
              VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(new LexicalLeg("cows", 15));
      assertQuery(plan, "q", RerankingQuery.Source.OPTIONS);
      assertPassage(plan, "body");
    }

    @Test
    void lexicalOnlyWithoutVector() {
      assertThat(LEXICAL_NO_VECTOR.vectorConfig().vectorEnabled()).isFalse();
      assertThat(LEXICAL_NO_VECTOR.lexicalDef().enabled()).isTrue();

      // {"$hybrid": {"$lexical": "cows"}}
      var plan =
          plan(LegMode.HYBRID, true, sort(null, "cows", null), RERANK_OPTIONS, LEXICAL_NO_VECTOR);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS);
    }

    @Test
    void explicitNullLexicalWithoutLexicalIndex() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": null}}, not an error, there is no value
      var plan = plan(LegMode.HYBRID, true, sort("cheese", null, null), VECTORIZE_NO_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void hybridLimitsNumber() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan =
          plan(
              LegMode.HYBRID,
              true,
              sort(null, "cows", VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 30}",
              VECTOR_LEXICAL);

      assertThat(plan.legs())
          .containsExactly(new LexicalLeg("cows", 30), new VectorLeg(VECTOR, 30));
    }

    @Test
    void hybridLimitsObject() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan =
          plan(
              LegMode.HYBRID,
              true,
              sort(null, "cows", VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 40, '$lexical': 15}}",
              VECTOR_LEXICAL);

      assertThat(plan.legs())
          .containsExactly(new LexicalLeg("cows", 15), new VectorLeg(VECTOR, 40));
    }
  }

  @Nested
  class QueryPassageAndLimit {

    @Test
    void limitFromOptions() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              LegMode.HYBRID,
              false,
              sort(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'limit': 5}",
              VECTOR_LEXICAL);

      assertThat(plan.limit()).isEqualTo(5);
    }

    @Test
    void rerankOnReplacesVectorizeField() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan =
          plan(
              LegMode.HYBRID,
              false,
              sort("cheese", null, null),
              "{'rerankOn': 'body'}",
              VECTORIZE_LEXICAL);

      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
      assertPassage(plan, "body");
    }

    @Test
    void blankRerankOptionsUseVectorize() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan =
          plan(
              LegMode.HYBRID,
              false,
              sort("cheese", null, null),
              "{'rerankQuery': '   ', 'rerankOn': '  '}",
              VECTORIZE_LEXICAL);

      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
      assertPassage(plan, DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    }
  }

  @Nested
  class RerankService {

    @Test
    void overrideOnRerankDisabledCollection() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              LegMode.HYBRID,
              false,
              sort(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '%s'}}"
                  .formatted(RERANK_MODEL),
              VECTOR_NO_RERANK);

      assertThat(plan.rerankServiceDef())
          .isEqualTo(new CollectionRerankDef.RerankServiceDef("nvidia", RERANK_MODEL, null, null));
    }

    @Test
    void overrideReplacesCollectionService() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              LegMode.HYBRID,
              false,
              sort(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '%s'}}"
                  .formatted(OTHER_RERANK_MODEL),
              VECTOR_LEXICAL);

      assertThat(plan.rerankServiceDef())
          .isEqualTo(
              new CollectionRerankDef.RerankServiceDef("nvidia", OTHER_RERANK_MODEL, null, null));
    }

    @Test
    void emptyOverride() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.INVALID_RERANK_OVERRIDE,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {}}",
                  VECTOR_LEXICAL));
    }

    @Test
    void rerankDisabledWithoutOverride() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  RERANK_OPTIONS,
                  VECTOR_NO_RERANK));
    }

    @Test
    void endOfLifeCollectionModel() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.END_OF_LIFE_AI_MODEL,
          () ->
              FindAndRerankPlanner.plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  options(RERANK_OPTIONS),
                  VECTOR_LEXICAL,
                  OPERATIONS_CONFIG,
                  providersConfig(ApiModelSupport.SupportStatus.END_OF_LIFE)));
    }
  }

  @Nested
  class Errors {

    @Test
    void lexicalOnlyMissingRerankQuery() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              plan(
                  LegMode.HYBRID,
                  true,
                  sort(null, "cows", null),
                  "{'rerankOn': 'body'}",
                  LEXICAL_NO_VECTOR));
    }

    @Test
    void lexicalOnlyMissingRerankOn() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () ->
              plan(
                  LegMode.HYBRID,
                  true,
                  sort(null, "cows", null),
                  "{'rerankQuery': 'q'}",
                  LEXICAL_NO_VECTOR));
    }

    @Test
    void explicitLexicalWithoutLexicalIndex() {
      var expectedMessage =
          "The collection without a lexical index: %s.%s."
              .formatted(TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME);

      // {"$hybrid": {"$lexical": "cows"}}
      var lexicalOnly =
          assertPlanFails(
              SchemaException.class,
              SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
              () ->
                  plan(
                      LegMode.HYBRID,
                      true,
                      sort(null, "cows", null),
                      RERANK_OPTIONS,
                      VECTORIZE_NO_LEXICAL));
      assertThat(lexicalOnly.getMessage()).contains(expectedMessage);

      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
          () -> plan(LegMode.HYBRID, true, sort("cheese", "cows", null), VECTORIZE_NO_LEXICAL));
    }

    @Test
    void vectorSortWithoutVector() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  RERANK_OPTIONS,
                  LEXICAL_NO_VECTOR));

      // {"$hybrid": {"$vectorize": "cheese"}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort("cheese", null, null),
                  RERANK_OPTIONS,
                  LEXICAL_NO_VECTOR));
    }

    @Test
    void vectorizeSortWithoutVectorizeService() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort("cheese", null, null),
                  RERANK_OPTIONS,
                  VECTOR_LEXICAL));

      // {"$hybrid": {"$vectorize": "cheese", "$vector": [0.1, 0.2, 0.3]}}, $vector does not help
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort("cheese", null, VECTOR),
                  RERANK_OPTIONS,
                  VECTOR_LEXICAL));
    }

    @Test
    void hybridLimitsOutOfBounds() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var vectorError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      LegMode.HYBRID,
                      true,
                      sort(null, "cows", VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
                      VECTOR_LEXICAL));
      assertThat(vectorError.getMessage())
          .contains("hybridLimits.$vector", "101", "must be between 1 and 100");

      var lexicalError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      LegMode.HYBRID,
                      true,
                      sort(null, "cows", VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': 0}}",
                      VECTOR_LEXICAL));
      assertThat(lexicalError.getMessage())
          .contains("hybridLimits.$lexical", "0", "must be between 1 and 100");
    }

    static Stream<Arguments> noHybridValueCases() {
      return Stream.of(
          Arguments.of("{\"$hybrid\": \"\"} or {\"$hybrid\": {}}", false, VECTOR_LEXICAL),
          Arguments.of("{\"$hybrid\": {\"$lexical\": null}}", true, VECTOR_LEXICAL),
          Arguments.of(
              "{\"$hybrid\": {\"$lexical\": null}} without lexical index",
              true,
              VECTORIZE_NO_LEXICAL));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noHybridValueCases")
    void noHybridValue(String request, boolean explicitLexical, CollectionSchemaObject collection) {
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_HYBRID_SORT_VALUE,
          () ->
              plan(
                  LegMode.HYBRID,
                  explicitLexical,
                  sort(null, null, null),
                  RERANK_OPTIONS,
                  collection));
    }

    @ParameterizedTest
    @EnumSource(
        value = LegMode.class,
        names = {"HYBRID"},
        mode = EnumSource.Mode.EXCLUDE)
    void unsupportedModes(LegMode legMode) {
      // the sort has the value for the mode, but only HYBRID is supported for now
      var sort =
          switch (legMode) {
            case HYBRID, FILTER -> sort(null, null, null);
            case VECTORIZE -> sort("cheese", null, null);
            case VECTOR -> sort(null, null, VECTOR);
            case LEXICAL -> sort(null, "cows", null);
          };

      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT,
          () -> plan(legMode, false, sort, RERANK_OPTIONS, VECTORIZE_LEXICAL));
    }

    @Test
    void emptyCommandMissingRerankQuery() throws JsonProcessingException {
      var command = OBJECT_MAPPER.readValue("{\"findAndRerank\": {}}", FindAndRerankCommand.class);

      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              FindAndRerankPlanner.plan(
                  LegMode.FILTER,
                  false,
                  command.sortClause(),
                  command.options(),
                  VECTOR_LEXICAL,
                  OPERATIONS_CONFIG,
                  PROVIDERS_CONFIG));
    }

    @Test
    void emptySortMissingRerankOn() {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () ->
              plan(
                  LegMode.FILTER,
                  false,
                  sort(null, null, null),
                  "{'rerankQuery': 'q'}",
                  VECTOR_LEXICAL));
    }
  }

  /** The first check that fails throws, these pin which error wins when there are several. */
  @Nested
  class ErrorOrder {

    @Test
    void vectorDisabledBeforeMissingRerankQuery() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(LegMode.HYBRID, false, sort(null, null, VECTOR), LEXICAL_NO_VECTOR));
    }

    @Test
    void vectorBeforeVectorizeBeforeLexicalIndex() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      var sort = sort("cheese", "cows", null);

      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(LegMode.HYBRID, true, sort, RERANK_OPTIONS, NOTHING_NO_RERANK));
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(LegMode.HYBRID, true, sort, RERANK_OPTIONS, VECTOR_NO_RERANK));
    }

    @Test
    void lexicalIndexBeforeHybridLimitsAndRerankService() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
          () ->
              plan(
                  LegMode.HYBRID,
                  true,
                  sort(null, "cows", null),
                  "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0}",
                  NOTHING_NO_RERANK));
    }

    @Test
    void hybridLimitsVectorBeforeLexical() {
      // {"$hybrid": {"$lexical": "cows"}}, the $vector value is checked even with no vector leg
      var ex =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      LegMode.HYBRID,
                      true,
                      sort(null, "cows", null),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 500, '$lexical': 0}}",
                      LEXICAL_NO_VECTOR));
      assertThat(ex.getMessage()).contains("hybridLimits.$vector");
    }

    @Test
    void hybridLimitsCheckedForLegsThatDoNotRun() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var ex =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      LegMode.HYBRID,
                      false,
                      sort(null, null, VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 10, '$lexical': 0}}",
                      VECTOR_LEXICAL));
      assertThat(ex.getMessage()).contains("hybridLimits.$lexical");
    }

    @Test
    void hybridLimitsBeforeRerankService() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
                  VECTOR_NO_RERANK));

      assertPlanFails(
          RequestException.class,
          RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0, 'rerank': {}}",
                  VECTOR_LEXICAL));
    }

    @Test
    void rerankDisabledBeforeMissingRerankOn() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, VECTOR),
                  "{'rerankQuery': 'q'}",
                  VECTOR_NO_RERANK));
    }

    @Test
    void rerankDisabledBeforeUnsupportedMode() {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () ->
              plan(
                  LegMode.FILTER,
                  false,
                  sort(null, null, null),
                  RERANK_OPTIONS,
                  NOTHING_NO_RERANK));
    }

    @Test
    void missingRerankQueryBeforeMissingRerankOn() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              plan(
                  LegMode.HYBRID,
                  true,
                  sort(null, "cows", VECTOR),
                  "{'hybridLimits': 20}",
                  VECTOR_LEXICAL));
    }

    @Test
    void rerankQueryAndPassageBeforeNoHybridValue() {
      // {"$hybrid": ""}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, null),
                  "{'rerankOn': 'body'}",
                  VECTOR_LEXICAL));
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, null),
                  "{'rerankQuery': 'q'}",
                  VECTOR_LEXICAL));
    }

    @Test
    void invalidRerankOnPathBeforeNoHybridValue() {
      // {"$hybrid": ""} with rerankOn 'a&b', a lone '&' is not a valid path
      assertPlanFails(
          UpdateException.class,
          UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH,
          () ->
              plan(
                  LegMode.HYBRID,
                  false,
                  sort(null, null, null),
                  "{'rerankQuery': 'q', 'rerankOn': 'a&b'}",
                  VECTOR_LEXICAL));
    }

    @Test
    void rerankQueryBeforeUnsupportedMode() {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              plan(
                  LegMode.FILTER,
                  false,
                  sort(null, null, null),
                  "{'rerankOn': 'body'}",
                  VECTOR_LEXICAL));
    }
  }

  @Nested
  class InnerReads {

    private final FilterDefinition filter = filter("{'name': 'cheese'}");

    @Test
    void lexicalLeg() {
      var leg = new LexicalLeg("cows", 20);
      assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.BM25);
      assertThat(leg.needsVectorize()).isFalse();
      assertThat(leg.defaultRerankQuery()).isEmpty();
      assertThat(leg.defaultPassageField()).isEmpty();

      var read = leg.buildInnerRead(filter, true, true);

      assertThat(read.deferredVectorize()).isNull();
      assertFindCommand(read.findCommand(), 20, false, true);
      var sortExpressions = sortClause(read.findCommand()).sortExpressions();
      assertThat(sortExpressions).hasSize(1);
      assertThat(sortExpressions.getFirst().isLexicalSort()).isTrue();
      assertThat(sortExpressions.getFirst().getLexicalQuery()).isEqualTo("cows");
    }

    @Test
    void vectorLeg() {
      var leg = new VectorLeg(VECTOR, 40);
      assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.VECTOR);
      assertThat(leg.needsVectorize()).isFalse();
      assertThat(leg.defaultRerankQuery()).isEmpty();
      assertThat(leg.defaultPassageField()).isEmpty();

      var read = leg.buildInnerRead(filter, true, false);

      assertThat(read.deferredVectorize()).isNull();
      assertFindCommand(read.findCommand(), 40, true, false);
      var sortExpressions = sortClause(read.findCommand()).sortExpressions();
      assertThat(sortExpressions).hasSize(1);
      assertThat(sortExpressions.getFirst().getVector()).containsExactly(VECTOR);
    }

    @Test
    void vectorizeLeg() {
      var leg = new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30);
      assertThat(leg.rankSource()).isEqualTo(Rank.RankSource.VECTOR);
      assertThat(leg.needsVectorize()).isTrue();
      assertThat(leg.defaultRerankQuery()).contains("cheese");
      assertThat(leg.defaultPassageField())
          .contains(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);

      var read = leg.buildInnerRead(filter, true, true);

      assertThat(read.deferredVectorize()).isNotNull();
      assertFindCommand(read.findCommand(), 30, true, true);
      var sortClause = sortClause(read.findCommand());
      assertThat(sortClause.sortExpressions()).as("no sort until the vector is ready").isEmpty();

      // the deferred vectorize updates the sort clause the find command was built with
      var embedding = new float[] {0.4f, 0.5f, 0.6f};
      ((EmbeddingDeferredAction) read.deferredVectorize().deferredAction()).onSuccess(embedding);
      assertThat(sortClause.sortExpressions()).hasSize(1);
      assertThat(sortClause.sortExpressions().getFirst().getVector()).containsExactly(embedding);
    }

    @Test
    void vectorizeLegBuildsNewReadEachTime() {
      var leg = new VectorizeLeg("cheese", DIMENSION, VECTORIZE_DEF, 30);

      var first = leg.buildInnerRead(filter, false, false);
      var second = leg.buildInnerRead(filter, false, false);

      assertThat(second.deferredVectorize()).isNotSameAs(first.deferredVectorize());
      assertThat(sortClause(second.findCommand())).isNotSameAs(sortClause(first.findCommand()));
    }

    private void assertFindCommand(
        FindCommand findCommand,
        int expectedLimit,
        boolean expectedIncludeSimilarity,
        boolean expectedIncludeSortVector) {

      assertThat(findCommand.filterDefinition()).as("filter").isSameAs(filter);
      assertThat(findCommand.projectionDefinition())
          .as("include all projection")
          .isEqualTo(OBJECT_MAPPER.createObjectNode().put("*", 1));
      assertThat(findCommand.options())
          .as("find options")
          .isEqualTo(
              new FindCommand.Options(
                  expectedLimit, 0, null, expectedIncludeSimilarity, expectedIncludeSortVector));
    }
  }

  @Nested
  class Plan {

    @Test
    void legsMustNotBeEmpty() {
      var rerankingQuery = RerankingQuery.create("q", null);
      var passageLocator = PathMatchLocator.forPath("body");

      var ex =
          assertThrowsExactly(
              IllegalArgumentException.class,
              () ->
                  new FindAndRerankPlan(
                      List.of(), COLLECTION_RERANK_SERVICE, rerankingQuery, passageLocator, 10));
      assertThat(ex.getMessage()).isEqualTo("legs must not be empty");
    }

    @Test
    void legsAreCopied() {
      var legs = new ArrayList<RerankLeg>(List.of(LEXICAL_COWS));
      var plan =
          new FindAndRerankPlan(
              legs,
              COLLECTION_RERANK_SERVICE,
              RerankingQuery.create("q", null),
              PathMatchLocator.forPath("body"),
              10);
      legs.add(VECTOR_LEG);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS);
    }
  }

  // ===========================================================================================
  // Helpers
  // ===========================================================================================

  private static FindAndRerankPlan plan(
      LegMode legMode,
      boolean explicitLexical,
      FindAndRerankSort sort,
      CollectionSchemaObject collection) {
    return plan(legMode, explicitLexical, sort, NO_OPTIONS, collection);
  }

  private static FindAndRerankPlan plan(
      LegMode legMode,
      boolean explicitLexical,
      FindAndRerankSort sort,
      String optionsJson,
      CollectionSchemaObject collection) {
    return FindAndRerankPlanner.plan(
        legMode,
        explicitLexical,
        sort,
        options(optionsJson),
        collection,
        OPERATIONS_CONFIG,
        PROVIDERS_CONFIG);
  }

  private static <T extends APIException> T assertPlanFails(
      Class<T> exceptionClass, Enum<?> expectedCode, Executable plan) {
    var ex = assertThrowsExactly(exceptionClass, plan);
    assertThat(ex.code).as("error code").isEqualTo(expectedCode.name());
    return ex;
  }

  private static void assertQuery(
      FindAndRerankPlan plan, String expectedQuery, RerankingQuery.Source expectedSource) {
    assertThat(plan.rerankingQuery().query()).as("rerank query").isEqualTo(expectedQuery);
    assertThat(plan.rerankingQuery().source()).as("rerank query source").isEqualTo(expectedSource);
  }

  private static void assertPassage(FindAndRerankPlan plan, String expectedField) {
    assertThat(plan.passageLocator().path()).as("passage field").isEqualTo(expectedField);
  }

  /** The values the parser gives for a sort, with a new set of features. */
  private static FindAndRerankSort sort(String vectorize, String lexical, float[] vector) {
    return new FindAndRerankSort(vectorize, lexical, vector, CommandFeatures.create());
  }

  /** Parses the options, the JSON uses single quotes to stay readable. */
  private static FindAndRerankCommand.Options options(String json) {
    if (json == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.readValue(json.replace('\'', '"'), FindAndRerankCommand.Options.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("invalid options JSON: " + json, e);
    }
  }

  private static FilterDefinition filter(String json) {
    try {
      return new FilterDefinition(OBJECT_MAPPER.readTree(json.replace('\'', '"')));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("invalid filter JSON: " + json, e);
    }
  }

  /** The wrapped clause is returned as is, the context is only used to parse a JSON sort. */
  @SuppressWarnings("unchecked")
  private static SortClause sortClause(FindCommand findCommand) {
    return findCommand.sortDefinition().build(mock(CommandContext.class));
  }

  private static CollectionSchemaObject collection(
      boolean vector, boolean vectorize, boolean lexical, boolean rerank) {

    var vectorConfig =
        vector
            ? VectorConfig.fromColumnDefinitions(
                List.of(
                    new VectorColumnDefinition(
                        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                        DIMENSION,
                        SimilarityFunction.COSINE,
                        EmbeddingSourceModel.OTHER,
                        vectorize ? VECTORIZE_DEF : null)))
            : VectorConfig.NOT_ENABLED_CONFIG;
    var lexicalFactory =
        lexical
            ? CollectionLexicalDefSchemaFactory.FOR_TESTING_ENABLED
            : CollectionLexicalDefSchemaFactory.FOR_TESTING_DISABLED;
    var rerankDef =
        rerank
            ? CollectionRerankDefSchemaFactory.FOR_TESTING_ENABLED.currentVersion(
                new CollectionRerankDef(true, COLLECTION_RERANK_SERVICE))
            : CollectionRerankDefSchemaFactory.FOR_TESTING_DISABLED.currentVersion(null);

    return new CollectionSchemaObject(
        TEST_CONSTANTS.COLLECTION_IDENTIFIER,
        IdConfig.defaultIdConfig(),
        vectorConfig,
        null,
        lexicalFactory.currentVersion(null),
        rerankDef);
  }

  private static OperationsConfig operationsConfig() {
    var config = mock(OperationsConfig.class);
    when(config.hybridSearchVectorLimit()).thenReturn(new IntConfigWithBounds(1, 20, 100));
    when(config.hybridSearchLexicalLimit()).thenReturn(new IntConfigWithBounds(1, 20, 100));
    when(config.defaultFindAndRerankLimit()).thenReturn(CONFIG_LIMIT);
    return config;
  }

  /**
   * A real providers config, a mock has no providers and would reject every override.
   *
   * @param collectionModelStatus Status of the model the collections use.
   */
  private static RerankingProvidersConfig providersConfig(
      ApiModelSupport.SupportStatus collectionModelStatus) {
    return new RerankingProvidersConfigImpl(
        Map.of(
            "nvidia",
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
                false,
                "nvidia",
                true,
                Map.of(),
                List.of(
                    modelConfig(RERANK_MODEL, collectionModelStatus),
                    modelConfig(OTHER_RERANK_MODEL, ApiModelSupport.SupportStatus.SUPPORTED)))));
  }

  private static RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
      modelConfig(String name, ApiModelSupport.SupportStatus status) {
    return new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
        name,
        new ApiModelSupport.ApiModelSupportImpl(status, Optional.empty()),
        false,
        "https://example.com/rerank",
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
            .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10));
  }
}
