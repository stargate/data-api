package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.config.IntConfigWithBounds;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.RerankingConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.operation.reranking.RerankingQuery;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link FindAndRerankPlanner}: the legs it plans for each kind of sort, and the error
 * for each check.
 */
class FindAndRerankPlannerTest {

  private static final String RERANK_OPTIONS =
      """
      {"rerankQuery": "q", "rerankOn": "body"}
      """;

  /** The sort of the rerank service and options cases, it can run on VECTOR_LEXICAL. */
  private static final String VECTOR_SORT =
      """
      {"$hybrid": {"$vector": [0.1, 0.2, 0.3]}}
      """;

  /** The vector in the sorts of the cases. */
  private static final float[] VECTOR = {0.1f, 0.2f, 0.3f};

  private static final int DEFAULT_READ_LIMIT = RerankingConstants.HybridSearchLimits.DEFAULT;

  // The lines of the errors that name the collection, formatted with the keyspace and collection
  private static final String NO_VECTOR_LINE =
      "The collection %s.%s does not have vectors enabled.";
  private static final String NO_VECTORIZE_LINE =
      "The collection %s.%s does not have vectorize enabled.";
  private static final String NO_LEXICAL_INDEX_LINE =
      "The collection without a lexical index: %s.%s.";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  // ===========================================================================================
  // Legs
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("legCases")
  void plansLegs(
      String description,
      String sort,
      String options,
      Collection collection,
      String vectorizeText,
      List<FindAndRerankPlan.RerankLeg> legs)
      throws Exception {
    var plan = FindAndRerankPlanner.plan(command(sort, options), commandContext(collection));

    assertThat(plan.vectorize() == null ? null : plan.vectorize().text())
        .as("%s: text to vectorize", description)
        .isEqualTo(vectorizeText);
    assertThat(plan.legs()).as("%s: legs", description).containsExactlyInAnyOrderElementsOf(legs);
  }

  static Stream<Arguments> legCases() {
    return Stream.of(
        Arguments.of(
            "$hybrid text, collection with a lexical index",
            """
            {"$hybrid": "cheese"}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT), lexicalLeg("cheese", DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$hybrid text, collection without a lexical index",
            """
            {"$hybrid": "cheese"}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$vectorize",
            """
            {"$hybrid": {"$vectorize": "cheese"}}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$vector",
            VECTOR_SORT,
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$vectorize and $vector, $vectorize is used",
            """
            {"$hybrid": {"$vectorize": "cheese", "$vector": [0.1, 0.2, 0.3]}}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$vectorize and $lexical",
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT), lexicalLeg("cows", DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$vector and $lexical",
            """
            {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
            """,
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(DEFAULT_READ_LIMIT), lexicalLeg("cows", DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$lexical null, no lexical index needed",
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": null}}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "$lexical empty, no lexical index needed",
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": ""}}
            """,
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            "cheese",
            List.of(vectorizedLeg(DEFAULT_READ_LIMIT))),
        Arguments.of(
            "hybridLimits for each leg",
            """
            {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
            """,
            """
            {"rerankQuery": "q", "rerankOn": "body",
             "hybridLimits": {"$vector": 100, "$lexical": 25}}
            """,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(100), lexicalLeg("cows", 25))),
        Arguments.of(
            "hybridLimits as one number for both legs",
            """
            {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
            """,
            """
            {"rerankQuery": "q", "rerankOn": "body", "hybridLimits": 30}
            """,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(30), lexicalLeg("cows", 30))),
        Arguments.of(
            "hybridLimits at the min of the bounds",
            """
            {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
            """,
            """
            {"rerankQuery": "q", "rerankOn": "body", "hybridLimits": 1}
            """,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(1), lexicalLeg("cows", 1))),
        Arguments.of(
            "hybridLimits at the max of the bounds",
            """
            {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
            """,
            """
            {"rerankQuery": "q", "rerankOn": "body", "hybridLimits": 100}
            """,
            Collection.VECTOR_LEXICAL,
            null,
            List.of(vectorLeg(100), lexicalLeg("cows", 100))));
  }

  // ===========================================================================================
  // Sort errors
  // ===========================================================================================

  /** The collection supports every kind of sort, so only the sort itself is wrong. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("sortErrorCases")
  void rejectsSort(String description, String sort, SortException.Code code) throws Exception {
    assertPlanFails(
        description,
        command(sort, RERANK_OPTIONS),
        Collection.VECTORIZE_LEXICAL,
        SortException.class,
        code);
  }

  static Stream<Arguments> sortErrorCases() {
    return Stream.of(
        Arguments.of(
            "$hybrid text empty",
            """
            {"$hybrid": ""}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$hybrid text blank",
            """
            {"$hybrid": "   "}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$hybrid object empty",
            """
            {"$hybrid": {}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$vectorize null",
            """
            {"$hybrid": {"$vectorize": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$vector null",
            """
            {"$hybrid": {"$vector": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$lexical null",
            """
            {"$hybrid": {"$lexical": null}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$lexical empty",
            """
            {"$hybrid": {"$lexical": ""}}
            """,
            SortException.Code.MISSING_HYBRID_SORT_VALUE),
        Arguments.of(
            "$lexical only",
            """
            {"$hybrid": {"$lexical": "cows"}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            "$lexical with $vectorize null",
            """
            {"$hybrid": {"$lexical": "cows", "$vectorize": null}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            "$lexical with $vectorize blank",
            """
            {"$hybrid": {"$lexical": "cows", "$vectorize": "   "}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of(
            "$lexical with $vector null",
            """
            {"$hybrid": {"$lexical": "cows", "$vector": null}}
            """,
            SortException.Code.MISSING_VECTOR_OR_VECTORIZE_IN_HYBRID_SORT),
        Arguments.of("no sort", null, SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT),
        Arguments.of(
            "sort empty",
            """
            {}
            """,
            SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT),
        Arguments.of(
            "sort null",
            """
            null
            """,
            SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT));
  }

  // ===========================================================================================
  // Sorts the collection cannot run
  // ===========================================================================================

  /** The error names the collection, the line is one of the *_LINE formats. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("collectionSupportErrorCases")
  void rejectsSortTheCollectionCannotRun(
      String description,
      String sort,
      Collection collection,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code,
      String collectionLine)
      throws Exception {
    var ex =
        assertPlanFails(
            description, command(sort, RERANK_OPTIONS), collection, exceptionClass, code);

    assertThat(ex)
        .as(description)
        .hasMessageContaining(
            collectionLine.formatted(TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME));
  }

  static Stream<Arguments> collectionSupportErrorCases() {
    return Stream.of(
        Arguments.of(
            "$hybrid text, collection without vectorize",
            """
            {"$hybrid": "cheese"}
            """,
            Collection.VECTOR_LEXICAL,
            SortException.class,
            SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
            NO_VECTORIZE_LINE),
        Arguments.of(
            "$vectorize, collection without vectorize",
            """
            {"$hybrid": {"$vectorize": "cheese"}}
            """,
            Collection.VECTOR_LEXICAL,
            SortException.class,
            SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION,
            NO_VECTORIZE_LINE),
        Arguments.of(
            "$hybrid text, collection without vector",
            """
            {"$hybrid": "cheese"}
            """,
            Collection.LEXICAL_NO_VECTOR,
            SortException.class,
            SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
            NO_VECTOR_LINE),
        Arguments.of(
            "$vectorize, collection without vector",
            """
            {"$hybrid": {"$vectorize": "cheese"}}
            """,
            Collection.LEXICAL_NO_VECTOR,
            SortException.class,
            SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
            NO_VECTOR_LINE),
        Arguments.of(
            "$vector, collection without vector",
            VECTOR_SORT,
            Collection.LEXICAL_NO_VECTOR,
            SortException.class,
            SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
            NO_VECTOR_LINE),
        Arguments.of(
            "$lexical, collection without a lexical index",
            """
            {"$hybrid": {"$vectorize": "cheese", "$lexical": "cows"}}
            """,
            Collection.VECTORIZE_NO_LEXICAL,
            SchemaException.class,
            SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
            NO_LEXICAL_INDEX_LINE));
  }

  // ===========================================================================================
  // Rerank service
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("rerankServiceErrorCases")
  void rejectsRerankService(
      String description,
      String options,
      Collection collection,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code)
      throws Exception {
    assertPlanFails(description, command(VECTOR_SORT, options), collection, exceptionClass, code);
  }

  static Stream<Arguments> rerankServiceErrorCases() {
    return Stream.of(
        Arguments.of(
            "collection without reranking, no rerank option",
            RERANK_OPTIONS,
            Collection.VECTOR_NO_RERANK,
            RequestException.class,
            RequestException.Code.UNSUPPORTED_RERANKING_COMMAND),
        Arguments.of(
            "rerank option empty",
            """
            {"rerankQuery": "q", "rerankOn": "body", "rerank": {}}
            """,
            Collection.VECTOR_LEXICAL,
            RequestException.class,
            RequestException.Code.INVALID_RERANK_OVERRIDE),
        Arguments.of(
            "collection model is end of life",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL_EOL_MODEL,
            SchemaException.class,
            SchemaException.Code.END_OF_LIFE_AI_MODEL));
  }

  @Test
  void usesRerankOptionWhenCollectionHasNoReranking() throws Exception {
    var options =
            """
        {"rerankQuery": "q", "rerankOn": "body",
         "rerank": {"provider": "nvidia", "modelName": "%s"}}
        """
            .formatted(TestConstants.RERANK_MODEL);

    var plan =
        FindAndRerankPlanner.plan(
            command(VECTOR_SORT, options), commandContext(Collection.VECTOR_NO_RERANK));

    assertThat(plan.rerank().rerankServiceDef().provider()).isEqualTo("nvidia");
    assertThat(plan.rerank().rerankServiceDef().modelName()).isEqualTo(TestConstants.RERANK_MODEL);
  }

  // ===========================================================================================
  // Options
  // ===========================================================================================

  @ParameterizedTest(name = "{0}")
  @MethodSource("optionErrorCases")
  void rejectsOptions(
      String description,
      String options,
      String projection,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code)
      throws Exception {
    assertPlanFails(
        description,
        command(VECTOR_SORT, options, projection),
        Collection.VECTOR_LEXICAL,
        exceptionClass,
        code);
  }

  static Stream<Arguments> optionErrorCases() {
    return Stream.of(
        Arguments.of(
            "no rerankQuery, and no $vectorize text to use instead",
            """
            {"rerankOn": "body"}
            """,
            null,
            RequestException.class,
            RequestException.Code.MISSING_RERANK_QUERY_TEXT),
        Arguments.of(
            "no rerankOn, and no $vectorize field to rerank on",
            """
            {"rerankQuery": "q"}
            """,
            null,
            RequestException.class,
            RequestException.Code.MISSING_RERANK_ON),
        Arguments.of(
            "rerankOn blank, and no $vectorize field to rerank on",
            """
            {"rerankQuery": "q", "rerankOn": "   "}
            """,
            null,
            RequestException.class,
            RequestException.Code.MISSING_RERANK_ON),
        Arguments.of(
            "rerankOn is not a valid path",
            """
            {"rerankQuery": "q", "rerankOn": "a&b"}
            """,
            null,
            UpdateException.class,
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH),
        Arguments.of(
            "projection is not valid",
            RERANK_OPTIONS,
            """
            {"$foo": 1}
            """,
            ProjectionException.class,
            ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM));
  }

  /** hybridLimits outside the bounds in the config, the error names the field and the value. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("hybridLimitsErrorCases")
  void rejectsHybridLimitsOutsideBounds(String description, String options, String fieldAndValue)
      throws Exception {
    var ex =
        assertPlanFails(
            description,
            command(VECTOR_SORT, options),
            Collection.VECTOR_LEXICAL,
            RequestException.class,
            RequestException.Code.COMMAND_FIELD_VALUE_INVALID);

    assertThat(ex)
        .as(description)
        .hasMessageContaining(fieldAndValue)
        .hasMessageContaining("must be between 1 and 100 (inclusive)");
  }

  static Stream<Arguments> hybridLimitsErrorCases() {
    return Stream.of(
        Arguments.of(
            "$vector limit above the max",
            """
            {"rerankQuery": "q", "rerankOn": "body",
             "hybridLimits": {"$vector": 101, "$lexical": 50}}
            """,
            "Command field 'hybridLimits.$vector' value 101 not valid"),
        Arguments.of(
            "$lexical limit above the max",
            """
            {"rerankQuery": "q", "rerankOn": "body",
             "hybridLimits": {"$vector": 50, "$lexical": 101}}
            """,
            "Command field 'hybridLimits.$lexical' value 101 not valid"),
        Arguments.of(
            "one limit for both legs below the min",
            """
            {"rerankQuery": "q", "rerankOn": "body", "hybridLimits": 0}
            """,
            "Command field 'hybridLimits.$vector' value 0 not valid"));
  }

  @Test
  void usesVectorizeTextWhenRerankOptionsAreBlank() throws Exception {
    var plan =
        FindAndRerankPlanner.plan(
            command(
                """
                {"$hybrid": {"$vectorize": "cheese"}}
                """,
                """
                {"rerankQuery": "   ", "rerankOn": "  "}
                """),
            commandContext(Collection.VECTORIZE_NO_LEXICAL));

    assertThat(plan.rerank().rerankingQuery().query()).isEqualTo("cheese");
    assertThat(plan.rerank().rerankingQuery().source()).isEqualTo(RerankingQuery.Source.VECTORIZE);
    assertThat(plan.rerank().passageLocator().path()).isEqualTo(VECTOR_EMBEDDING_TEXT_FIELD);
  }

  // ===========================================================================================
  // Error order
  // ===========================================================================================

  /**
   * The one error order the planner keeps on purpose, see the NOTE in {@link
   * FindAndRerankPlanner#plan}: the options are checked before the legs are planned. This command
   * has no rerank query and a $hybrid sort with no value, it fails for the rerank query.
   */
  @Test
  void reportsMissingRerankQueryBeforeHybridSortWithNoValue() throws Exception {
    assertPlanFails(
        "no rerank query and an empty $hybrid sort",
        command(
            """
            {"$hybrid": ""}
            """,
            """
            {"rerankOn": "body"}
            """),
        Collection.VECTOR_LEXICAL,
        RequestException.class,
        RequestException.Code.MISSING_RERANK_QUERY_TEXT);
  }

  // ===========================================================================================
  // Whole plan
  // ===========================================================================================

  @Test
  void plansHybridTextWithDefaults() throws Exception {
    var plan =
        FindAndRerankPlanner.plan(
            command(
                """
                {"$hybrid": " cheese "}
                """,
                null),
            commandContext(Collection.VECTORIZE_LEXICAL));

    var vectorizeColumn =
        TEST_CONSTANTS
            .VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT
            .vectorConfig()
            .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
            .orElseThrow();
    assertThat(plan.vectorize())
        .isEqualTo(
            new FindAndRerankPlan.VectorizeStep(
                "cheese", vectorizeColumn.vectorSize(), vectorizeColumn.vectorizeDefinition()));
    assertThat(plan.legs())
        .containsExactlyInAnyOrder(
            vectorizedLeg(DEFAULT_READ_LIMIT), lexicalLeg("cheese", DEFAULT_READ_LIMIT));

    var rerank = plan.rerank();
    assertThat(rerank.rerankServiceDef())
        .isEqualTo(
            new CollectionRerankDef.RerankServiceDef(
                "nvidia", TestConstants.RERANK_MODEL, null, null));
    assertThat(rerank.rerankingQuery().query()).isEqualTo("cheese");
    assertThat(rerank.rerankingQuery().source()).isEqualTo(RerankingQuery.Source.VECTORIZE);
    assertThat(rerank.passageLocator().path()).isEqualTo(VECTOR_EMBEDDING_TEXT_FIELD);
    assertThat(rerank.limit()).isEqualTo(OperationsConfig.DEFAULT_FIND_AND_RERANK_LIMIT);
    assertThat(plan.includeScores()).isFalse();
    assertThat(plan.includeSortVector()).isFalse();
  }

  @Test
  void plansVectorWithOptions() throws Exception {
    var plan =
        FindAndRerankPlanner.plan(
            command(
                """
                {"$hybrid": {"$vector": [0.1, 0.2, 0.3], "$lexical": "cows"}}
                """,
                """
                {"rerankQuery": "q", "rerankOn": "body", "limit": 5,
                 "hybridLimits": {"$vector": 20, "$lexical": 30},
                 "includeScores": true, "includeSortVector": true}
                """),
            commandContext(Collection.VECTOR_LEXICAL));

    assertThat(plan.vectorize()).isNull();
    assertThat(plan.legs()).containsExactlyInAnyOrder(vectorLeg(20), lexicalLeg("cows", 30));

    var rerank = plan.rerank();
    assertThat(rerank.rerankingQuery().query()).isEqualTo("q");
    assertThat(rerank.rerankingQuery().source()).isEqualTo(RerankingQuery.Source.OPTIONS);
    assertThat(rerank.passageLocator().path()).isEqualTo("body");
    assertThat(rerank.limit()).isEqualTo(5);
    assertThat(plan.includeScores()).isTrue();
    assertThat(plan.includeSortVector()).isTrue();
  }

  // ===========================================================================================
  // Helpers / no more tests below
  // ===========================================================================================

  /** Asserts planning fails with the exception and code, returns the exception for more checks. */
  private APIException assertPlanFails(
      String description,
      FindAndRerankCommand command,
      Collection collection,
      Class<? extends APIException> exceptionClass,
      ErrorCode<?> code) {
    var commandContext = commandContext(collection);

    var ex =
        assertThrowsExactly(
            exceptionClass, () -> FindAndRerankPlanner.plan(command, commandContext), description);
    assertThat(ex.code).as(description).isEqualTo(code.name());
    return ex;
  }

  private FindAndRerankCommand command(String sort, String options) throws Exception {
    return command(sort, options, null);
  }

  /** A findAndRerank command, a <code>null</code> sort, options or projection is left out. */
  private FindAndRerankCommand command(String sort, String options, String projection)
      throws Exception {
    var fields = new ArrayList<String>();
    if (sort != null) {
      fields.add("\"sort\": " + sort);
    }
    if (projection != null) {
      fields.add("\"projection\": " + projection);
    }
    if (options != null) {
      fields.add("\"options\": " + options);
    }
    return objectMapper.readValue(
        "{\"findAndRerank\": {" + String.join(", ", fields) + "}}", FindAndRerankCommand.class);
  }

  private CommandContext<CollectionSchemaObject> commandContext(Collection collection) {
    var commandContext =
        TEST_CONSTANTS.collectionContext(
            CommandName.FIND_AND_RERANK, schema(collection), commandConfig());

    // a real providers config, so rerank overrides are validated the same way as in production
    var modelStatus =
        collection == Collection.VECTOR_LEXICAL_EOL_MODEL
            ? ApiModelSupport.SupportStatus.END_OF_LIFE
            : ApiModelSupport.SupportStatus.SUPPORTED;
    when(commandContext.rerankingProviderFactory().getRerankingConfig())
        .thenReturn(
            rerankingProvidersConfig(
                "nvidia", true, rerankingModelConfig(TestConstants.RERANK_MODEL, modelStatus)));
    return commandContext;
  }

  /** The config the planner reads, with the default hybridLimits bounds and limit. */
  private static CommandConfig commandConfig() {
    var hybridLimitBounds =
        new IntConfigWithBounds(
            RerankingConstants.HybridSearchLimits.MIN,
            RerankingConstants.HybridSearchLimits.DEFAULT,
            RerankingConstants.HybridSearchLimits.MAX);
    var operationsConfig = mock(OperationsConfig.class);
    when(operationsConfig.hybridSearchVectorLimit()).thenReturn(hybridLimitBounds);
    when(operationsConfig.hybridSearchLexicalLimit()).thenReturn(hybridLimitBounds);
    when(operationsConfig.defaultFindAndRerankLimit())
        .thenReturn(OperationsConfig.DEFAULT_FIND_AND_RERANK_LIMIT);

    var commandConfig = mock(CommandConfig.class);
    when(commandConfig.get(OperationsConfig.class)).thenReturn(operationsConfig);
    return commandConfig;
  }

  private CollectionSchemaObject schema(Collection collection) {
    return switch (collection) {
      case VECTORIZE_LEXICAL -> TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;
      case VECTORIZE_NO_LEXICAL -> TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT;
      case VECTOR_LEXICAL, VECTOR_LEXICAL_EOL_MODEL ->
          TEST_CONSTANTS.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;
      case LEXICAL_NO_VECTOR -> TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT;
      case VECTOR_NO_RERANK -> TEST_CONSTANTS.VECTOR_COLLECTION_SCHEMA_OBJECT;
    };
  }

  /** A VECTOR_LEG that reads with the vector from the vectorize step. */
  private static FindAndRerankPlan.RerankLeg vectorizedLeg(int readLimit) {
    return new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, null, null, readLimit);
  }

  /** A VECTOR_LEG that reads with {@link #VECTOR}. */
  private static FindAndRerankPlan.RerankLeg vectorLeg(int readLimit) {
    return new FindAndRerankPlan.RerankLeg(RerankLegType.VECTOR_LEG, VECTOR, null, readLimit);
  }

  private static FindAndRerankPlan.RerankLeg lexicalLeg(String lexicalText, int readLimit) {
    return new FindAndRerankPlan.RerankLeg(RerankLegType.LEXICAL_LEG, null, lexicalText, readLimit);
  }

  /** The collection configs the cases run against. */
  private enum Collection {
    /** vector, vectorize, lexical and rerank */
    VECTORIZE_LEXICAL,
    /** vector, vectorize and rerank, no lexical index */
    VECTORIZE_NO_LEXICAL,
    /** vector without vectorize, lexical and rerank */
    VECTOR_LEXICAL,
    /** Same schema as VECTOR_LEXICAL, the providers config says the collection model is EOL. */
    VECTOR_LEXICAL_EOL_MODEL,
    /** lexical and rerank, no vector */
    LEXICAL_NO_VECTOR,
    /** vector without vectorize, no lexical index, rerank disabled */
    VECTOR_NO_RERANK
  }
}
