package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.FindAndRerankSort;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.LegMode;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.IntConfigWithBounds;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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
 * <p>The sorts are built with the values the parser gives for each request, the JSON of the request
 * is in the comments. The collections are the schema objects from {@link TestConstants}.
 */
public class FindAndRerankPlannerTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String OTHER_RERANK_MODEL = "nvidia/other-rerank-model";
  private static final CollectionRerankDef.RerankServiceDef COLLECTION_RERANK_SERVICE =
      new CollectionRerankDef.RerankServiceDef("nvidia", TestConstants.RERANK_MODEL, null, null);
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

  // the bounds of hybridLimits, and a config default that differs from HybridLimits.DEFAULT
  private static final int HYBRID_LIMIT_MIN = RerankingConstants.HybridSearchLimits.MIN;
  private static final int HYBRID_LIMIT_MAX = RerankingConstants.HybridSearchLimits.MAX;
  private static final int HYBRID_LIMIT_CONFIG_DEFAULT = 20;

  // config values that differ from the production defaults, to show where each value comes from
  private static final int CONFIG_LIMIT = 7;
  private static final OperationsConfig OPERATIONS_CONFIG = operationsConfig();
  private static final RerankingProvidersConfig PROVIDERS_CONFIG =
      providersConfig(ApiModelSupport.SupportStatus.SUPPORTED);

  // read limit when the command has no hybridLimits, never HYBRID_LIMIT_CONFIG_DEFAULT
  private static final int DEFAULT_READ_LIMIT =
      FindAndRerankCommand.HybridLimits.DEFAULT.vectorLimit();

  /** vector + vectorize + lexical + rerank */
  private static final CollectionSchemaObject VECTORIZE_LEXICAL =
      TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;

  /** vector + vectorize + rerank, no lexical index */
  private static final CollectionSchemaObject VECTORIZE_NO_LEXICAL =
      TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT;

  /** vector (no vectorize) + lexical + rerank */
  private static final CollectionSchemaObject VECTOR_LEXICAL =
      TEST_CONSTANTS.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;

  /** lexical + rerank, no vector */
  private static final CollectionSchemaObject LEXICAL_NO_VECTOR =
      TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT;

  /** vector (no vectorize), no lexical, rerank disabled */
  private static final CollectionSchemaObject VECTOR_NO_RERANK =
      TEST_CONSTANTS.VECTOR_COLLECTION_SCHEMA_OBJECT;

  /** no vector, no lexical, rerank disabled */
  private static final CollectionSchemaObject NOTHING_NO_RERANK =
      TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT_LEGACY;

  // the vectorize leg takes the dimension and the vectorize service from this column
  private static final VectorColumnDefinition VECTORIZE_COLUMN =
      VECTORIZE_LEXICAL
          .vectorConfig()
          .getColumnDefinition(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
          .orElseThrow();

  private static final String NO_OPTIONS = null;
  private static final String RERANK_OPTIONS = "{'rerankQuery': 'q', 'rerankOn': 'body'}";

  private static final LexicalLeg LEXICAL_COWS = new LexicalLeg("cows", DEFAULT_READ_LIMIT);
  private static final VectorizeLeg VECTORIZE_CHEESE =
      new VectorizeLeg(
          "cheese",
          VECTORIZE_COLUMN.vectorSize(),
          VECTORIZE_COLUMN.vectorizeDefinition(),
          DEFAULT_READ_LIMIT);
  private static final VectorLeg VECTOR_LEG = new VectorLeg(VECTOR, DEFAULT_READ_LIMIT);

  @Nested
  class HybridLegs {

    @Test
    void hybridTextWithLexicalIndexRunsLexicalThenVectorizeLeg() {
      // {"$hybrid": "cheese"}
      var plan = plan(hybridText("cheese"), VECTORIZE_LEXICAL);

      assertThat(plan.legs())
          .as("lexical leg first, read limits from HybridLimits.DEFAULT")
          .containsExactly(new LexicalLeg("cheese", DEFAULT_READ_LIMIT), VECTORIZE_CHEESE);
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
      var plan = plan(hybridText("cheese"), VECTORIZE_NO_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void hybridTextReadsEachHybridLimit() {
      // {"$hybrid": "cheese"}
      var plan =
          plan(
              hybridText("cheese"),
              "{'hybridLimits': {'$vector': 40, '$lexical': 15}}",
              VECTORIZE_LEXICAL);

      assertThat(plan.legs())
          .containsExactly(
              new LexicalLeg("cheese", 15),
              new VectorizeLeg(
                  "cheese",
                  VECTORIZE_COLUMN.vectorSize(),
                  VECTORIZE_COLUMN.vectorizeDefinition(),
                  40));
    }

    @Test
    void vectorizeOnlyRunsVectorizeLeg() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan = plan(hybridObject("cheese", null, null), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void vectorOnlyRunsVectorLeg() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan = plan(hybridObject(null, null, VECTOR), RERANK_OPTIONS, VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTOR_LEG);
      assertQuery(plan, "q", RerankingQuery.Source.OPTIONS);
      assertPassage(plan, "body");
    }

    @Test
    void vectorizeWinsOverVector() {
      // {"$hybrid": {"$vectorize": "cheese", "$vector": [0.1, 0.2, 0.3]}}
      var plan = plan(hybridObject("cheese", null, VECTOR), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void vectorizeAndLexicalRunsLexicalThenVectorizeLeg() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      var plan = plan(hybridObject("cheese", "cows", null), VECTORIZE_LEXICAL);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS, VECTORIZE_CHEESE);
      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
    }

    @Test
    void vectorAndLexicalRunsLexicalThenVectorLeg() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan = plan(hybridObject(null, "cows", VECTOR), RERANK_OPTIONS, VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS, VECTOR_LEG);
    }

    @Test
    void lexicalOnlyReadsLexicalHybridLimit() {
      // {"$hybrid": {"$lexical": "cows"}}
      var plan =
          plan(
              hybridObject(null, "cows", null),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 30, '$lexical': 15}}",
              VECTOR_LEXICAL);

      assertThat(plan.legs()).containsExactly(new LexicalLeg("cows", 15));
      assertQuery(plan, "q", RerankingQuery.Source.OPTIONS);
      assertPassage(plan, "body");
    }

    @Test
    void lexicalOnlyWithoutVectorRunsLexicalLeg() {
      assertThat(LEXICAL_NO_VECTOR.vectorConfig().vectorEnabled()).isFalse();
      assertThat(LEXICAL_NO_VECTOR.lexicalDef().enabled()).isTrue();

      // {"$hybrid": {"$lexical": "cows"}}
      var plan = plan(hybridObject(null, "cows", null), RERANK_OPTIONS, LEXICAL_NO_VECTOR);

      assertThat(plan.legs()).containsExactly(LEXICAL_COWS);
    }

    @Test
    void explicitNullLexicalWithoutLexicalIndexSkipsLexicalLeg() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": null}}, not an error, there is no value
      var plan = plan(hybridObject("cheese", null, null), VECTORIZE_NO_LEXICAL);

      assertThat(plan.legs()).containsExactly(VECTORIZE_CHEESE);
    }

    @Test
    void hybridLimitsNumberSetsBothReadLimits() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan =
          plan(
              hybridObject(null, "cows", VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 30}",
              VECTOR_LEXICAL);

      assertThat(plan.legs())
          .containsExactly(new LexicalLeg("cows", 30), new VectorLeg(VECTOR, 30));
    }

    @Test
    void hybridLimitsObjectSetsEachReadLimit() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var plan =
          plan(
              hybridObject(null, "cows", VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 40, '$lexical': 15}}",
              VECTOR_LEXICAL);

      assertThat(plan.legs())
          .containsExactly(new LexicalLeg("cows", 15), new VectorLeg(VECTOR, 40));
    }

    @Test
    void failExplicitLexicalWithoutLexicalIndex() {
      var expectedMessage =
          "The collection without a lexical index: %s.%s."
              .formatted(TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME);

      // {"$hybrid": {"$lexical": "cows"}}
      var lexicalOnlyError =
          assertPlanFails(
              SchemaException.class,
              SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
              () -> plan(hybridObject(null, "cows", null), RERANK_OPTIONS, VECTORIZE_NO_LEXICAL));
      assertThat(lexicalOnlyError.getMessage()).contains(expectedMessage);

      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
          () -> plan(hybridObject("cheese", "cows", null), VECTORIZE_NO_LEXICAL));
    }

    @Test
    void failVectorSortWithoutVector() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(hybridObject(null, null, VECTOR), RERANK_OPTIONS, LEXICAL_NO_VECTOR));

      // {"$hybrid": {"$vectorize": "cheese"}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(hybridObject("cheese", null, null), RERANK_OPTIONS, LEXICAL_NO_VECTOR));

      // {"$hybrid": "cheese"}, the collection has a lexical index but the text needs vectorize
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(hybridText("cheese"), RERANK_OPTIONS, LEXICAL_NO_VECTOR));
    }

    @Test
    void failVectorizeSortWithoutVectorizeService() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(hybridObject("cheese", null, null), RERANK_OPTIONS, VECTOR_LEXICAL));

      // {"$hybrid": {"$vectorize": "cheese", "$vector": [0.1, 0.2, 0.3]}}, $vector does not help
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(hybridObject("cheese", null, VECTOR), RERANK_OPTIONS, VECTOR_LEXICAL));

      // {"$hybrid": "cheese"}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(hybridText("cheese"), RERANK_OPTIONS, VECTOR_LEXICAL));
    }

    @Test
    void failHybridLimitsOutOfBounds() {
      var expectedBounds =
          "must be between %d and %d".formatted(HYBRID_LIMIT_MIN, HYBRID_LIMIT_MAX);

      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      var vectorError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      hybridObject(null, "cows", VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': %d}"
                          .formatted(HYBRID_LIMIT_MAX + 1),
                      VECTOR_LEXICAL));
      assertThat(vectorError.getMessage())
          .contains(invalidLimit("hybridLimits.$vector", HYBRID_LIMIT_MAX + 1), expectedBounds);

      var lexicalError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      hybridObject(null, "cows", VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': %d}}"
                          .formatted(HYBRID_LIMIT_MIN - 1),
                      VECTOR_LEXICAL));
      assertThat(lexicalError.getMessage())
          .contains(invalidLimit("hybridLimits.$lexical", HYBRID_LIMIT_MIN - 1), expectedBounds);
    }

    static Stream<Arguments> noHybridValueCases() {
      return Stream.of(
          Arguments.of("{\"$hybrid\": \"\"}", hybridText(null), VECTOR_LEXICAL),
          Arguments.of("{\"$hybrid\": \"\"} without vector", hybridText(null), LEXICAL_NO_VECTOR),
          Arguments.of(
              "{\"$hybrid\": {}} or {\"$hybrid\": {\"$lexical\": null}}",
              hybridObject(null, null, null),
              VECTOR_LEXICAL),
          Arguments.of(
              "{\"$hybrid\": {\"$lexical\": null}} without lexical index",
              hybridObject(null, null, null),
              VECTORIZE_NO_LEXICAL));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("noHybridValueCases")
    void failNoHybridValue(
        String request, FindAndRerankSort sort, CollectionSchemaObject collection) {
      assertPlanFails(
          SortException.class,
          SortException.Code.MISSING_HYBRID_SORT_VALUE,
          () -> plan(sort, RERANK_OPTIONS, collection));
    }

    @Test
    void failUnsupportedMode() {
      // {}, null or no sort
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT,
          () -> plan(FindAndRerankSort.NO_ARG_SORT, RERANK_OPTIONS, VECTORIZE_LEXICAL));
    }

    @ParameterizedTest
    @EnumSource(
        value = LegMode.class,
        names = {"VECTORIZE", "VECTOR", "LEXICAL"})
    void failSingleLegModes(LegMode legMode) {
      // the parser does not produce these modes yet (GH #2602), the sort has the value for the mode
      var sort =
          switch (legMode) {
            case VECTORIZE -> singleLegSort(legMode, "cheese", null, null);
            case VECTOR -> singleLegSort(legMode, null, null, VECTOR);
            case LEXICAL -> singleLegSort(legMode, null, "cows", null);
            case HYBRID, FILTER -> throw new IllegalArgumentException("not a single leg mode");
          };

      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT,
          () -> plan(sort, RERANK_OPTIONS, VECTORIZE_LEXICAL));
    }

    private static FindAndRerankSort singleLegSort(
        LegMode legMode, String vectorize, String lexical, float[] vector) {
      return new FindAndRerankSort(
          legMode, null, vectorize, lexical, vector, CommandFeatures.create());
    }
  }

  @Nested
  class QueryPassageAndLimit {

    @Test
    void optionsLimitReplacesConfigLimit() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              hybridObject(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'limit': 5}",
              VECTOR_LEXICAL);

      assertThat(plan.limit()).isEqualTo(5);
    }

    @Test
    void rerankOnReplacesVectorizeField() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan =
          plan(hybridObject("cheese", null, null), "{'rerankOn': 'body'}", VECTORIZE_LEXICAL);

      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
      assertPassage(plan, "body");
    }

    @Test
    void blankRerankOptionsUseVectorize() {
      // {"$hybrid": {"$vectorize": "cheese"}}
      var plan =
          plan(
              hybridObject("cheese", null, null),
              "{'rerankQuery': '   ', 'rerankOn': '  '}",
              VECTORIZE_LEXICAL);

      assertQuery(plan, "cheese", RerankingQuery.Source.VECTORIZE);
      assertPassage(plan, DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    }

    @Test
    void failLexicalOnlyWithoutRerankQuery() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () -> plan(hybridObject(null, "cows", null), "{'rerankOn': 'body'}", LEXICAL_NO_VECTOR));
    }

    @Test
    void failLexicalOnlyWithoutRerankOn() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () -> plan(hybridObject(null, "cows", null), "{'rerankQuery': 'q'}", LEXICAL_NO_VECTOR));
    }
  }

  @Nested
  class RerankService {

    @Test
    void overrideOnRerankDisabledCollectionIsUsed() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              hybridObject(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '%s'}}"
                  .formatted(TestConstants.RERANK_MODEL),
              VECTOR_NO_RERANK);

      assertThat(plan.rerankServiceDef())
          .isEqualTo(
              new CollectionRerankDef.RerankServiceDef(
                  "nvidia", TestConstants.RERANK_MODEL, null, null));
    }

    @Test
    void overrideReplacesCollectionService() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      var plan =
          plan(
              hybridObject(null, null, VECTOR),
              "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '%s'}}"
                  .formatted(OTHER_RERANK_MODEL),
              VECTOR_LEXICAL);

      assertThat(plan.rerankServiceDef())
          .isEqualTo(
              new CollectionRerankDef.RerankServiceDef("nvidia", OTHER_RERANK_MODEL, null, null));
    }

    @Test
    void failEmptyOverride() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.INVALID_RERANK_OVERRIDE,
          () ->
              plan(
                  hybridObject(null, null, VECTOR),
                  "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {}}",
                  VECTOR_LEXICAL));
    }

    @Test
    void failRerankDisabledWithoutOverride() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () -> plan(hybridObject(null, null, VECTOR), RERANK_OPTIONS, VECTOR_NO_RERANK));
    }

    @Test
    void failEndOfLifeCollectionModel() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.END_OF_LIFE_AI_MODEL,
          () ->
              FindAndRerankPlanner.plan(
                  hybridObject(null, null, VECTOR),
                  options(RERANK_OPTIONS),
                  VECTOR_LEXICAL,
                  OPERATIONS_CONFIG,
                  providersConfig(ApiModelSupport.SupportStatus.END_OF_LIFE)));
    }
  }

  /**
   * The first check that fails throws, these pin which error wins when a request has several. See
   * the javadoc of {@link FindAndRerankPlanner#plan}.
   */
  @Nested
  class ErrorOrder {

    @Test
    void vectorDisabledBeforeMissingRerankQuery() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(hybridObject(null, null, VECTOR), LEXICAL_NO_VECTOR));
    }

    @Test
    void vectorBeforeVectorizeBeforeLexicalIndex() {
      // {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
      var sort = hybridObject("cheese", "cows", null);

      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(sort, RERANK_OPTIONS, NOTHING_NO_RERANK));
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(sort, RERANK_OPTIONS, VECTOR_NO_RERANK));
    }

    @Test
    void hybridTextVectorBeforeVectorize() {
      // {"$hybrid": "cheese"}
      var sort = hybridText("cheese");

      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
          () -> plan(sort, RERANK_OPTIONS, NOTHING_NO_RERANK));
      assertPlanFails(
          SortException.class,
          SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
          () -> plan(sort, RERANK_OPTIONS, VECTOR_NO_RERANK));
    }

    @Test
    void lexicalIndexBeforeHybridLimitsAndRerankService() {
      // {"$hybrid": {"$lexical": "cows"}}
      assertPlanFails(
          SchemaException.class,
          SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
          () ->
              plan(
                  hybridObject(null, "cows", null),
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
                      hybridObject(null, "cows", null),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 500, '$lexical': 0}}",
                      LEXICAL_NO_VECTOR));
      assertThat(ex.getMessage()).contains(invalidLimit("hybridLimits.$vector", 500));
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
                      hybridObject(null, null, VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 10, '$lexical': 0}}",
                      VECTOR_LEXICAL));
      assertThat(ex.getMessage()).contains(invalidLimit("hybridLimits.$lexical", 0));
    }

    @Test
    void hybridLimitsBeforeRerankService() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}, on a collection with reranking disabled
      var rerankDisabledError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      hybridObject(null, null, VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': %d}"
                          .formatted(HYBRID_LIMIT_MAX + 1),
                      VECTOR_NO_RERANK));
      assertThat(rerankDisabledError.getMessage())
          .contains(invalidLimit("hybridLimits.$vector", HYBRID_LIMIT_MAX + 1));

      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}, with an empty rerank override
      var emptyOverrideError =
          assertPlanFails(
              RequestException.class,
              RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
              () ->
                  plan(
                      hybridObject(null, null, VECTOR),
                      "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': %d, 'rerank': {}}"
                          .formatted(HYBRID_LIMIT_MIN - 1),
                      VECTOR_LEXICAL));
      assertThat(emptyOverrideError.getMessage())
          .contains(invalidLimit("hybridLimits.$vector", HYBRID_LIMIT_MIN - 1));
    }

    @Test
    void rerankDisabledBeforeMissingRerankOn() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () -> plan(hybridObject(null, null, VECTOR), "{'rerankQuery': 'q'}", VECTOR_NO_RERANK));
    }

    @Test
    void rerankDisabledBeforeUnsupportedMode() {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
          () -> plan(FindAndRerankSort.NO_ARG_SORT, RERANK_OPTIONS, NOTHING_NO_RERANK));
    }

    @Test
    void missingRerankQueryBeforeMissingRerankOn() {
      // {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () -> plan(hybridObject(null, "cows", VECTOR), "{'hybridLimits': 20}", VECTOR_LEXICAL));
    }

    @Test
    void rerankQueryAndPassageBeforeNoHybridValue() {
      // {"$hybrid": ""}
      var sort = hybridText(null);

      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () -> plan(sort, "{'rerankOn': 'body'}", VECTOR_LEXICAL));
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () -> plan(sort, "{'rerankQuery': 'q'}", VECTOR_LEXICAL));
    }

    @Test
    void invalidRerankOnPathBeforeNoHybridValue() {
      // {"$hybrid": ""} with rerankOn 'a&b', a lone '&' is not a valid path
      var ex =
          assertPlanFails(
              UpdateException.class,
              UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH,
              () ->
                  plan(
                      hybridText(null), "{'rerankQuery': 'q', 'rerankOn': 'a&b'}", VECTOR_LEXICAL));
      assertThat(ex.getMessage()).contains("a&b");
    }

    @Test
    void rerankQueryBeforeUnsupportedMode() throws JsonProcessingException {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () -> plan(FindAndRerankSort.NO_ARG_SORT, "{'rerankOn': 'body'}", VECTOR_LEXICAL));

      // the whole command is {"findAndRerank": {}}, no sort and no options
      var command = OBJECT_MAPPER.readValue("{\"findAndRerank\": {}}", FindAndRerankCommand.class);
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_QUERY_TEXT,
          () ->
              FindAndRerankPlanner.plan(
                  command.sortClause(),
                  command.options(),
                  VECTOR_LEXICAL,
                  OPERATIONS_CONFIG,
                  PROVIDERS_CONFIG));
    }

    @Test
    void rerankOnBeforeUnsupportedMode() {
      // {}
      assertPlanFails(
          RequestException.class,
          RequestException.Code.MISSING_RERANK_ON,
          () -> plan(FindAndRerankSort.NO_ARG_SORT, "{'rerankQuery': 'q'}", VECTOR_LEXICAL));
    }
  }

  // ===========================================================================================
  // Helpers
  // ===========================================================================================

  private static FindAndRerankPlan plan(FindAndRerankSort sort, CollectionSchemaObject collection) {
    return plan(sort, NO_OPTIONS, collection);
  }

  private static FindAndRerankPlan plan(
      FindAndRerankSort sort, String optionsJson, CollectionSchemaObject collection) {
    return FindAndRerankPlanner.plan(
        sort, options(optionsJson), collection, OPERATIONS_CONFIG, PROVIDERS_CONFIG);
  }

  private static <T extends APIException> T assertPlanFails(
      Class<T> exceptionClass, ErrorCode<T> expectedCode, Executable planCall) {
    var ex = assertThrowsExactly(exceptionClass, planCall);
    assertThat(ex.code).as("error code").isEqualTo(expectedCode.name());
    return ex;
  }

  /**
   * The start of the COMMAND_FIELD_VALUE_INVALID message for a <code>hybridLimits</code> value, it
   * has the field and the value together so a wrong value fails the assert.
   */
  private static String invalidLimit(String field, int value) {
    return "Command field '%s' value %d not valid".formatted(field, value);
  }

  private static void assertQuery(
      FindAndRerankPlan plan, String expectedQuery, RerankingQuery.Source expectedSource) {
    assertThat(plan.rerankingQuery().query()).as("rerank query").isEqualTo(expectedQuery);
    assertThat(plan.rerankingQuery().source()).as("rerank query source").isEqualTo(expectedSource);
  }

  private static void assertPassage(FindAndRerankPlan plan, String expectedField) {
    assertThat(plan.passageLocator().path()).as("passage field").isEqualTo(expectedField);
  }

  /**
   * The value the parser gives for <code>{"$hybrid": "text"}</code>, with a new set of features.
   *
   * @param text The trimmed text, <code>null</code> when the text is blank.
   */
  private static FindAndRerankSort hybridText(String text) {
    return FindAndRerankSort.fromHybridText(text, CommandFeatures.create());
  }

  /**
   * The values the parser gives for a <code>$hybrid</code> object, with a new set of features. A
   * value is <code>null</code> when the request does not have its key, or the key is null or blank.
   */
  private static FindAndRerankSort hybridObject(String vectorize, String lexical, float[] vector) {
    return FindAndRerankSort.fromHybridObject(vectorize, lexical, vector, CommandFeatures.create());
  }

  /** Parses the options, the JSON uses single quotes to stay readable. */
  private static FindAndRerankCommand.Options options(String json) {
    if (json == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER
          .readerFor(FindAndRerankCommand.Options.class)
          .with(JsonReadFeature.ALLOW_SINGLE_QUOTES)
          .readValue(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("invalid options JSON: " + json, e);
    }
  }

  private static OperationsConfig operationsConfig() {
    var limitBounds =
        new IntConfigWithBounds(HYBRID_LIMIT_MIN, HYBRID_LIMIT_CONFIG_DEFAULT, HYBRID_LIMIT_MAX);
    var config = mock(OperationsConfig.class);
    when(config.hybridSearchVectorLimit()).thenReturn(limitBounds);
    when(config.hybridSearchLexicalLimit()).thenReturn(limitBounds);
    when(config.defaultFindAndRerankLimit()).thenReturn(CONFIG_LIMIT);
    return config;
  }

  /**
   * The nvidia provider with the model the collections use and one other model.
   *
   * @param collectionModelStatus Status of the model the collections use.
   */
  private static RerankingProvidersConfig providersConfig(
      ApiModelSupport.SupportStatus collectionModelStatus) {
    return rerankingProvidersConfig(
        "nvidia",
        true,
        rerankingModelConfig(TestConstants.RERANK_MODEL, collectionModelStatus),
        rerankingModelConfig(OTHER_RERANK_MODEL, ApiModelSupport.SupportStatus.SUPPORTED));
  }
}
