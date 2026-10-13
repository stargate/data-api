package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.TestConstants.rerankingModelConfig;
import static io.stargate.sgv2.jsonapi.TestConstants.rerankingProvidersConfig;
import static io.stargate.sgv2.jsonapi.service.resolver.FindAndRerankOperationAssertions.readTaskSnapshots;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.reranking.Rank;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Behavior baseline for {@link FindAndRerankOperationBuilder#build()}: pins the outcome for many
 * request shapes and collection configs. A row either builds an operation, and then pins the reads
 * it runs (rank source and limit, in order) and the hybrid limits on the command context, or it
 * pins the error code.
 *
 * <p>Some rows pin behaviour that is easy to change by accident:
 *
 * <ul>
 *   <li>A <code>$hybrid</code> sort with only <code>$lexical</code> runs one lexical read (R26,
 *       R27, G17).
 *   <li>A <code>$hybrid</code> sort with no value fails with MISSING_HYBRID_SORT_VALUE (R05, G03,
 *       R08, R09, G02, R11), and a request with no sort fails with UNSUPPORTED_FIND_AND_RERANK_SORT
 *       (R31, R32, R33).
 *   <li>Those two sort errors come before the projection is checked, only O15 pins this for both.
 *   <li>The rerank query and rerankOn errors come before those two sort errors (R07, R34, G09, G10,
 *       R35, G21, O18).
 * </ul>
 *
 * <p>The row ids are stable labels for the test report, they carry no meaning of their own and gaps
 * are expected.
 *
 * <p>JSON in the rows uses single quotes to stay readable, the command is parsed with {@link
 * JsonReadFeature#ALLOW_SINGLE_QUOTES}.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderBehaviorTest {

  private static final String RERANK_OPTIONS = "{'rerankQuery': 'q', 'rerankOn': 'body'}";
  private static final String NO_SORT = null;
  private static final String NO_OPTIONS = null;
  private static final String VECTOR_JSON = "[0.1, 0.2, 0.3]";
  private static final String BAD_PROJECTION = "{'$foo': 1}";

  /** The collection configs used by the rows, named like the ones in FindAndRerankPlannerTest. */
  private enum Collection {
    /** vector + vectorize + lexical + rerank */
    VECTORIZE_LEXICAL,
    /** vector + vectorize + rerank, no lexical index */
    VECTORIZE_NO_LEXICAL,
    /** vector (no vectorize) + lexical + rerank */
    VECTOR_LEXICAL,
    /** Same schema as VECTOR_LEXICAL, but the providers config says the collection model is EOL. */
    VECTOR_LEXICAL_EOL_MODEL,
    /** lexical + rerank, no vector */
    LEXICAL_NO_VECTOR,
    /** vector (no vectorize), no lexical, rerank disabled */
    VECTOR_NO_RERANK,
    /** no vector, no lexical, rerank disabled */
    NOTHING_NO_RERANK
  }

  // @QuarkusTest needed for error template initialization and the OperationsConfig bounds
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  static Stream<Row> rows() {
    return Stream.of(
        // ---- $hybrid text ----
        row(
            "R01",
            "{'$hybrid': 'cheese'}",
            RERANK_OPTIONS,
            Collection.VECTORIZE_LEXICAL,
            success(lexicalRead(), vectorRead())),
        row(
            "R02",
            "{'$hybrid': 'cheese'}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "R03",
            "{'$hybrid': 'cheese'}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),
        row(
            "G05",
            "{'$hybrid': 'cheese'}",
            RERANK_OPTIONS,
            Collection.LEXICAL_NO_VECTOR,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "G16",
            "{'$hybrid': 'cheese'}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true}",
            Collection.VECTORIZE_LEXICAL,
            success(lexicalRead(), vectorRead())),
        row(
            "R05",
            "{'$hybrid': ''}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // a $hybrid sort with no value
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "G03",
            "{'$hybrid': '   '}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // a $hybrid sort with no value
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R07",
            "{'$hybrid': ''}",
            "{'rerankOn': 'body'}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),

        // ---- $hybrid object with no value ----
        row(
            "R08",
            "{'$hybrid': {}}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // a $hybrid sort with no value
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R09",
            "{'$hybrid': {'$lexical': null}}",
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            // an explicit null $lexical has no value, so the missing lexical index is not an error
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "G02",
            "{'$hybrid': {'$lexical': ''}}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // a $hybrid sort with no value
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R11",
            "{'$hybrid': {'$vectorize': null}}",
            RERANK_OPTIONS,
            Collection.LEXICAL_NO_VECTOR,
            // a $hybrid sort with no value
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),

        // ---- vector / vectorize legs ----
        row(
            "R13",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "G04",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            RERANK_OPTIONS,
            Collection.LEXICAL_NO_VECTOR,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "G14",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            RERANK_OPTIONS,
            Collection.NOTHING_NO_RERANK,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "R15",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            NO_OPTIONS,
            Collection.LEXICAL_NO_VECTOR,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "R16",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            success(vectorRead())),
        row(
            "G12",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'limit': 5}",
            "{'a': 1}",
            null,
            Collection.VECTOR_LEXICAL,
            success(vectorRead())),
        row(
            "G15",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankOn': 'body'}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R19",
            "{'$hybrid': {'$vectorize': 'cheese', '$vector': " + VECTOR_JSON + "}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "R20",
            "{'$hybrid': {'$vectorize': 'cheese', '$vector': " + VECTOR_JSON + "}}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),

        // ---- two legs, explicit $lexical ----
        row(
            "R21",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VECTORIZE_LEXICAL,
            success(lexicalRead(), vectorRead())),
        row(
            "G18",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            "{'rerankOn': 'body'}",
            Collection.VECTORIZE_LEXICAL,
            success(lexicalRead(), vectorRead())),
        row(
            "R22",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            failure(
                SchemaException.class, SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "R23",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': null}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "G01",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': ''}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "G19",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': null}}",
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "R24",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 100, '$lexical': 25}}",
            Collection.VECTOR_LEXICAL,
            success(100, 25, lexicalRead(25), vectorRead(100))),
        row(
            "G23",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 30}",
            Collection.VECTOR_LEXICAL,
            success(30, 30, lexicalRead(30), vectorRead(30))),

        // ---- lexical-only $hybrid ----
        row(
            "R26",
            "{'$hybrid': {'$lexical': 'cows'}}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // a lexical-only $hybrid runs one lexical read
            success(lexicalRead())),
        row(
            "R27",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': 20}}",
            Collection.LEXICAL_NO_VECTOR,
            // a lexical-only $hybrid runs one lexical read
            success(50, 20, lexicalRead(20))),
        row(
            "G17",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true}",
            Collection.LEXICAL_NO_VECTOR,
            // a lexical-only $hybrid runs one lexical read
            success(lexicalRead())),
        row(
            "R28",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankOn': 'body'}",
            Collection.LEXICAL_NO_VECTOR,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R29",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q'}",
            Collection.LEXICAL_NO_VECTOR,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_ON)),
        row(
            "R30",
            "{'$hybrid': {'$lexical': 'cows'}}",
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            failure(
                SchemaException.class, SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "G06",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 500, '$lexical': 10}}",
            Collection.LEXICAL_NO_VECTOR,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$vector")),

        // ---- no sort value: {}, null, missing ----
        row(
            "R31",
            "{}",
            RERANK_OPTIONS,
            Collection.VECTOR_LEXICAL,
            // no $hybrid sort
            failure(SortException.class, SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R32",
            "null",
            RERANK_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            // no $hybrid sort
            failure(SortException.class, SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R33",
            NO_SORT,
            RERANK_OPTIONS,
            Collection.LEXICAL_NO_VECTOR,
            // no $hybrid sort
            failure(SortException.class, SortException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R34",
            "{}",
            "{'rerankQuery': 'q'}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_ON)),
        row(
            "G09",
            "{}",
            "{'rerankOn': 'body'}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "G10",
            "null",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        // the whole command is {"findAndRerank": {}}, mirrors the HCD-only IT failOnEmptyRequest
        row(
            "R35",
            NO_SORT,
            NO_OPTIONS,
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R37",
            "{}",
            RERANK_OPTIONS,
            Collection.NOTHING_NO_RERANK,
            failure(RequestException.class, RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),
        row(
            "G20",
            NO_SORT,
            NO_OPTIONS,
            Collection.NOTHING_NO_RERANK,
            failure(RequestException.class, RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),

        // ---- options ----
        row(
            "R38",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerankQuery': '   ', 'rerankOn': '  '}",
            Collection.VECTORIZE_NO_LEXICAL,
            success(vectorRead())),
        row(
            "G07",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'hybridLimits': 30}",
            Collection.VECTORIZE_NO_LEXICAL,
            success(30, 30, vectorRead(30))),
        row(
            "R42",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0}",
            Collection.VECTOR_LEXICAL,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$vector")),
        row(
            "R43",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
            Collection.VECTOR_LEXICAL,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$vector")),
        row(
            "R44",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': 101}}",
            Collection.VECTOR_LEXICAL,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$lexical")),
        row(
            "R45",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 10, '$lexical': 0}}",
            Collection.VECTOR_LEXICAL,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$lexical")),
        row(
            "R46",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true, 'includeSortVector': true}",
            Collection.VECTOR_LEXICAL,
            success(lexicalRead(), vectorRead())),

        // ---- rerank service override ----
        row(
            "R47",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '"
                + TestConstants.RERANK_MODEL
                + "'}}",
            Collection.VECTOR_NO_RERANK,
            success(vectorRead())),
        row(
            "R48",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {}}",
            Collection.VECTOR_NO_RERANK,
            failure(RequestException.class, RequestException.Code.INVALID_RERANK_OVERRIDE)),
        row(
            "G11",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerank': {'provider': 'unknown', 'modelName': 'some-model'}}",
            Collection.VECTORIZE_NO_LEXICAL,
            failure(RequestException.class, RequestException.Code.INVALID_RERANK_OVERRIDE)),

        // ---- order of errors ----
        row(
            "O02",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q'}",
            Collection.VECTOR_NO_RERANK,
            failure(RequestException.class, RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),
        row(
            "O03",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
            Collection.VECTOR_NO_RERANK,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$vector")),
        row(
            "O04",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VECTORIZE_NO_LEXICAL,
            failure(
                SchemaException.class, SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "O05",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerankQuery': 'q', 'hybridLimits': 0}",
            Collection.VECTOR_LEXICAL,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),
        row(
            "O07",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.NOTHING_NO_RERANK,
            failure(
                SortException.class, SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "O08",
            "{'$hybrid': {'$lexical': 'cows'}}",
            RERANK_OPTIONS,
            Collection.NOTHING_NO_RERANK,
            failure(
                SchemaException.class, SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "O09",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0, 'rerank': {}}",
            Collection.VECTOR_LEXICAL,
            failure(
                RequestException.class,
                RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
                "hybridLimits.$vector")),
        row(
            "O10",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerank': {}}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.INVALID_RERANK_OVERRIDE)),
        row(
            "O11",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VECTOR_LEXICAL_EOL_MODEL,
            failure(SchemaException.class, SchemaException.Code.END_OF_LIFE_AI_MODEL)),
        row(
            "O12",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'hybridLimits': 20}",
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "O13",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q'}",
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_ON)),
        row(
            "O14",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + ", '$lexical': 'cows'}}",
            RERANK_OPTIONS,
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            failure(
                ProjectionException.class, ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM)),
        row(
            "O15",
            "{'$hybrid': ''}",
            RERANK_OPTIONS,
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            // the sort errors come before the projection error
            failure(SortException.class, SortException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "O16",
            "{'$hybrid': {'$lexical': 'cows'}}",
            RERANK_OPTIONS,
            null,
            BAD_PROJECTION,
            Collection.LEXICAL_NO_VECTOR,
            failure(
                ProjectionException.class, ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM)),
        row(
            "G21",
            "{'$hybrid': ''}",
            "{'rerankQuery': 'q'}",
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            failure(RequestException.class, RequestException.Code.MISSING_RERANK_ON)),
        // an invalid rerankOn path fails after the rerank query check, before the projection error
        // and before the no $hybrid value error
        row(
            "O17",
            "{'$hybrid': {'$vector': " + VECTOR_JSON + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'a&b'}",
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            failure(UpdateException.class, UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH)),
        row(
            "O18",
            "{'$hybrid': ''}",
            "{'rerankQuery': 'q', 'rerankOn': 'a&b'}",
            null,
            BAD_PROJECTION,
            Collection.VECTOR_LEXICAL,
            failure(
                UpdateException.class, UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("rows")
  void buildOutcome(Row row) throws Exception {
    var commandContext = commandContext(row.collection());
    var command =
        objectMapper
            .readerFor(FindAndRerankCommand.class)
            .with(JsonReadFeature.ALLOW_SINGLE_QUOTES)
            .<FindAndRerankCommand>readValue(row.commandJson());

    switch (row.expected()) {
      case Success success -> {
        var operation = builder(commandContext, command).build();
        assertThat(
                readTaskSnapshots(operation).stream()
                    .map(read -> new ExpectedRead(read.rankSource(), read.limit()))
                    .toList())
            .as(row.id() + " reads, in order")
            .containsExactlyElementsOf(success.reads());
        assertThat(commandContext.getHybridLimits().vectorLimit())
            .as(row.id() + " hybridLimits.$vector on the command context")
            .isEqualTo(success.vectorLimit());
        assertThat(commandContext.getHybridLimits().lexicalLimit())
            .as(row.id() + " hybridLimits.$lexical on the command context")
            .isEqualTo(success.lexicalLimit());
      }
      case Failure failure -> {
        var ex =
            assertThrowsExactly(
                failure.exceptionClass(), () -> builder(commandContext, command).build(), row.id());
        assertThat(ex.code).as(row.id()).isEqualTo(failure.code().name());
        if (failure.messageFragment() != null) {
          assertThat(ex.getMessage()).as(row.id()).contains(failure.messageFragment());
        }
      }
    }
  }

  private FindAndRerankOperationBuilder builder(
      CommandContext<CollectionSchemaObject> commandContext, FindAndRerankCommand command) {
    return new FindAndRerankOperationBuilder(commandContext)
        .withCommand(command)
        .withFindCommandResolver(findCommandResolver);
  }

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  private CollectionSchemaObject schema(Collection collection) {
    return switch (collection) {
      case VECTORIZE_LEXICAL -> TEST_CONSTANTS.VECTORIZE_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;
      case VECTORIZE_NO_LEXICAL -> TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT;
      case VECTOR_LEXICAL, VECTOR_LEXICAL_EOL_MODEL ->
          TEST_CONSTANTS.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;
      case LEXICAL_NO_VECTOR -> TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT;
      case VECTOR_NO_RERANK -> TEST_CONSTANTS.VECTOR_COLLECTION_SCHEMA_OBJECT;
      case NOTHING_NO_RERANK -> TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT_LEGACY;
    };
  }

  private CommandContext<CollectionSchemaObject> commandContext(Collection collection) {
    var schemaObject = schema(collection);
    var commandContext =
        TEST_CONSTANTS.collectionContext(CommandName.FIND_AND_RERANK, schemaObject);

    // a real providers config, so rerank overrides are validated the same way as in production
    var modelStatus =
        collection == Collection.VECTOR_LEXICAL_EOL_MODEL
            ? ApiModelSupport.SupportStatus.END_OF_LIFE
            : ApiModelSupport.SupportStatus.SUPPORTED;
    when(commandContext.rerankingProviderFactory().getRerankingConfig())
        .thenReturn(
            rerankingProvidersConfig(
                "nvidia", true, rerankingModelConfig(TestConstants.RERANK_MODEL, modelStatus)));
    when(commandContext.rerankingProviderFactory().create(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(RerankingProvider.class));

    if (schemaObject.vectorConfig().getFirstVectorColumnWithVectorizeDefinition().isPresent()) {
      when(commandContext
              .embeddingProviderFactory()
              .create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
          .thenReturn(mock(EmbeddingProvider.class));
    }
    return commandContext;
  }

  // ===========================================================================================
  // Rows
  // ===========================================================================================

  private static Row row(
      String id, String sort, String options, Collection collection, Expected expected) {
    return new Row(id, sort, options, null, null, collection, expected);
  }

  private static Row row(
      String id,
      String sort,
      String options,
      String filter,
      String projection,
      Collection collection,
      Expected expected) {
    return new Row(id, sort, options, filter, projection, collection, expected);
  }

  /** build() returns an operation with these reads, and the default hybrid limits. */
  private static Expected success(ExpectedRead... reads) {
    var defaults = FindAndRerankCommand.HybridLimits.DEFAULT;
    return new Success(defaults.vectorLimit(), defaults.lexicalLimit(), List.of(reads));
  }

  /** build() returns an operation with these reads, and these hybrid limits. */
  private static Expected success(int vectorLimit, int lexicalLimit, ExpectedRead... reads) {
    return new Success(vectorLimit, lexicalLimit, List.of(reads));
  }

  private static <T extends APIException> Expected failure(
      Class<T> exceptionClass, ErrorCode<T> code) {
    return new Failure(exceptionClass, code, null);
  }

  private static <T extends APIException> Expected failure(
      Class<T> exceptionClass, ErrorCode<T> code, String messageFragment) {
    return new Failure(exceptionClass, code, messageFragment);
  }

  /** A lexical read with the default lexical limit. */
  private static ExpectedRead lexicalRead() {
    return lexicalRead(FindAndRerankCommand.HybridLimits.DEFAULT.lexicalLimit());
  }

  private static ExpectedRead lexicalRead(int limit) {
    return new ExpectedRead(Rank.RankSource.BM25, limit);
  }

  /** A vector or vectorize read with the default vector limit. */
  private static ExpectedRead vectorRead() {
    return vectorRead(FindAndRerankCommand.HybridLimits.DEFAULT.vectorLimit());
  }

  private static ExpectedRead vectorRead(int limit) {
    return new ExpectedRead(Rank.RankSource.VECTOR, limit);
  }

  /**
   * One request shape against one collection config.
   *
   * @param sort the sort JSON, or null when the command has no sort key
   * @param options the options JSON, or null when the command has no options key
   * @param filter the filter JSON, or null when the command has no filter key
   * @param projection the projection JSON, or null when the command has no projection key
   */
  private record Row(
      String id,
      String sort,
      String options,
      String filter,
      String projection,
      Collection collection,
      Expected expected) {

    /** The command JSON, with single quotes. */
    String commandJson() {
      var members = new ArrayList<String>();
      if (filter != null) {
        members.add("'filter': " + filter);
      }
      if (sort != null) {
        members.add("'sort': " + sort);
      }
      if (projection != null) {
        members.add("'projection': " + projection);
      }
      if (options != null) {
        members.add("'options': " + options);
      }
      return "{'findAndRerank': {" + String.join(", ", members) + "}}";
    }

    @Override
    public String toString() {
      return "%s: sort=%s, options=%s, filter=%s, projection=%s on %s -> %s"
          .formatted(id, sort, options, filter, projection, collection, expected);
    }
  }

  private sealed interface Expected permits Success, Failure {}

  /**
   * build() returns an operation with these reads, and copies these hybrid limits onto the command
   * context.
   */
  private record Success(int vectorLimit, int lexicalLimit, List<ExpectedRead> reads)
      implements Expected {}

  /**
   * build() throws exactly this API exception class with this code, and optionally with this
   * message fragment.
   */
  private record Failure(
      Class<? extends APIException> exceptionClass, ErrorCode<?> code, String messageFragment)
      implements Expected {}

  /** A read task of the operation: the rank source of the deferred read it fills, and its limit. */
  private record ExpectedRead(Rank.RankSource rankSource, int limit) {}
}
