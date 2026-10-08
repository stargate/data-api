package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
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
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Behavior baseline for {@link FindAndRerankOperationBuilder#build()}: pins the outcome (success or
 * error code) for many request shapes and collection configs.
 *
 * <p>The rows were recorded on the builder before the leg refactor, and every row must keep its
 * expectation through the refactor. The only rows that changed are the ones marked "changed by the
 * leg refactor". Most of them are the two approved behavior changes: lexical-only <code>$hybrid
 * </code> requests run one lexical leg (issue #2577), and requests whose sort has no value to
 * search with get an explicit error. Before the refactor these rows threw an {@link
 * IllegalArgumentException}, which the API returned as an HTTP 500. One more row also changed (O15,
 * was UNSUPPORTED_PROJECTION_PARAM): when the sort has no value to search with, the new error now
 * comes before the projection error. This applies to both new errors, MISSING_HYBRID_SORT_VALUE and
 * UNSUPPORTED_FIND_AND_RERANK_SORT, but only O15 pins it.
 *
 * <p>The row ids are stable labels for the test report, they carry no meaning of their own and gaps
 * are expected.
 *
 * <p>JSON in the rows uses single quotes to stay readable, they are replaced with double quotes
 * before parsing.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindAndRerankOperationBuilderBehaviorTest {

  // @QuarkusTest needed for error template initialization and the OperationsConfig bounds
  @InjectMock protected RequestContext dataApiRequestInfo;

  @Inject ObjectMapper objectMapper;
  @Inject FindCommandResolver findCommandResolver;

  private final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final String RERANK_MODEL = "nvidia/llama-3.2-nv-rerankqa-1b-v2";

  /** The collection configs used by the rows. */
  enum Collection {
    /** vector + vectorize + lexical + rerank, built in this test. */
    VZ_LX_RR,
    /** vector + vectorize + rerank, no lexical index. */
    VZ_RR,
    /** vector (no vectorize) + lexical + rerank. */
    V_LX_RR,
    /** Same schema as V_LX_RR, but the providers config says the collection model is EOL. */
    V_LX_RR_EOL_MODEL,
    /** lexical + rerank, no vector. */
    LX_RR,
    /** vector (no vectorize), no lexical, rerank disabled. */
    V_NORR,
    /** no vector, no lexical, rerank disabled. */
    PLAIN_NORR
  }

  private static final String Q_ON = "{'rerankQuery': 'q', 'rerankOn': 'body'}";
  private static final String NO_SORT = null;
  private static final String NO_OPTIONS = null;
  private static final String VEC = "[0.1, 0.2, 0.3]";
  private static final String BAD_PROJECTION = "{'$foo': 1}";

  static Stream<Row> rows() {
    return Stream.of(
        // ---- $hybrid text ----
        row("R01", "{'$hybrid': 'cheese'}", Q_ON, Collection.VZ_LX_RR, success()),
        row("R02", "{'$hybrid': 'cheese'}", NO_OPTIONS, Collection.VZ_RR, success()),
        row(
            "R03",
            "{'$hybrid': 'cheese'}",
            Q_ON,
            Collection.V_LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),
        row(
            "G05",
            "{'$hybrid': 'cheese'}",
            Q_ON,
            Collection.LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "G16",
            "{'$hybrid': 'cheese'}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true}",
            Collection.VZ_LX_RR,
            success()),
        row(
            "R05",
            "{'$hybrid': ''}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): no $hybrid value gives
            // MISSING_HYBRID_SORT_VALUE
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "G03",
            "{'$hybrid': '   '}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): no $hybrid value gives
            // MISSING_HYBRID_SORT_VALUE
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R07",
            "{'$hybrid': ''}",
            "{'rerankOn': 'body'}",
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),

        // ---- $hybrid object with no value ----
        row(
            "R08",
            "{'$hybrid': {}}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): no $hybrid value gives
            // MISSING_HYBRID_SORT_VALUE
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R09",
            "{'$hybrid': {'$lexical': null}}",
            Q_ON,
            Collection.VZ_RR,
            // changed by the leg refactor (was HTTP 500): MISSING_HYBRID_SORT_VALUE, not
            // LEXICAL_NOT_ENABLED_FOR_COLLECTION
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "G02",
            "{'$hybrid': {'$lexical': ''}}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): no $hybrid value gives
            // MISSING_HYBRID_SORT_VALUE
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "R11",
            "{'$hybrid': {'$vectorize': null}}",
            Q_ON,
            Collection.LX_RR,
            // changed by the leg refactor (was HTTP 500): no $hybrid value gives
            // MISSING_HYBRID_SORT_VALUE
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),

        // ---- vector / vectorize legs ----
        row(
            "R13",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            success()),
        row(
            "G04",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            Q_ON,
            Collection.LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "G14",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            Q_ON,
            Collection.PLAIN_NORR,
            error(SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "R15",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            NO_OPTIONS,
            Collection.LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row("R16", "{'$hybrid': {'$vector': " + VEC + "}}", Q_ON, Collection.V_LX_RR, success()),
        row(
            "G12",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'limit': 5}",
            "{'a': 1}",
            null,
            Collection.V_LX_RR,
            success()),
        row(
            "G15",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankOn': 'body'}",
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R19",
            "{'$hybrid': {'$vectorize': 'cheese', '$vector': " + VEC + "}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            success()),
        row(
            "R20",
            "{'$hybrid': {'$vectorize': 'cheese', '$vector': " + VEC + "}}",
            Q_ON,
            Collection.V_LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),

        // ---- two legs, explicit $lexical ----
        row(
            "R21",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VZ_LX_RR,
            success()),
        row(
            "G18",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            "{'rerankOn': 'body'}",
            Collection.VZ_LX_RR,
            success()),
        row(
            "R22",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            error(SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "R23",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': null}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            success()),
        row(
            "G01",
            "{'$hybrid': {'$vectorize': 'cheese', '$lexical': ''}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            success()),
        row(
            "G19",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': null}}",
            Q_ON,
            Collection.VZ_RR,
            success()),
        row(
            "R24",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 100, '$lexical': 25}}",
            Collection.V_LX_RR,
            success(100, 25)),
        row(
            "G23",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 30}",
            Collection.V_LX_RR,
            success(30, 30)),

        // ---- lexical-only $hybrid ----
        row(
            "R26",
            "{'$hybrid': {'$lexical': 'cows'}}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): lexical-only $hybrid runs one lexical
            // leg (issue #2577)
            success()),
        row(
            "R27",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': 20}}",
            Collection.LX_RR,
            // changed by the leg refactor (was HTTP 500): lexical-only $hybrid runs one lexical
            // leg (issue #2577)
            success(50, 20)),
        row(
            "G17",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true}",
            Collection.LX_RR,
            // changed by the leg refactor (was HTTP 500): lexical-only $hybrid runs one lexical
            // leg (issue #2577)
            success()),
        row(
            "R28",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankOn': 'body'}",
            Collection.LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R29",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q'}",
            Collection.LX_RR,
            error(RequestException.Code.MISSING_RERANK_ON)),
        row(
            "R30",
            "{'$hybrid': {'$lexical': 'cows'}}",
            Q_ON,
            Collection.VZ_RR,
            error(SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "G06",
            "{'$hybrid': {'$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 500, '$lexical': 10}}",
            Collection.LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$vector")),

        // ---- no sort value: {}, null, missing ----
        row(
            "R31",
            "{}",
            Q_ON,
            Collection.V_LX_RR,
            // changed by the leg refactor (was HTTP 500): FILTER mode gives
            // UNSUPPORTED_FIND_AND_RERANK_SORT
            error(RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R32",
            "null",
            Q_ON,
            Collection.VZ_RR,
            // changed by the leg refactor (was HTTP 500): FILTER mode gives
            // UNSUPPORTED_FIND_AND_RERANK_SORT
            error(RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R33",
            NO_SORT,
            Q_ON,
            Collection.LX_RR,
            // changed by the leg refactor (was HTTP 500): FILTER mode gives
            // UNSUPPORTED_FIND_AND_RERANK_SORT
            error(RequestException.Code.UNSUPPORTED_FIND_AND_RERANK_SORT)),
        row(
            "R34",
            "{}",
            "{'rerankQuery': 'q'}",
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_ON)),
        row(
            "G09",
            "{}",
            "{'rerankOn': 'body'}",
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "G10",
            "null",
            NO_OPTIONS,
            Collection.VZ_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        // the whole command is {"findAndRerank": {}}, mirrors the HCD-only IT failOnEmptyRequest
        row(
            "R35",
            NO_SORT,
            NO_OPTIONS,
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "R37",
            "{}",
            Q_ON,
            Collection.PLAIN_NORR,
            error(RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),
        row(
            "G20",
            NO_SORT,
            NO_OPTIONS,
            Collection.PLAIN_NORR,
            error(RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),

        // ---- options ----
        row(
            "R38",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerankQuery': '   ', 'rerankOn': '  '}",
            Collection.VZ_RR,
            success()),
        row(
            "G07",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'hybridLimits': 30}",
            Collection.VZ_RR,
            success(30, 30)),
        row(
            "R42",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0}",
            Collection.V_LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$vector")),
        row(
            "R43",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
            Collection.V_LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$vector")),
        row(
            "R44",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 50, '$lexical': 101}}",
            Collection.V_LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$lexical")),
        row(
            "R45",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': {'$vector': 10, '$lexical': 0}}",
            Collection.V_LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$lexical")),
        row(
            "R46",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'includeScores': true, 'includeSortVector': true}",
            Collection.V_LX_RR,
            success()),

        // ---- rerank service override ----
        row(
            "R47",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {'provider': 'nvidia', 'modelName': '"
                + RERANK_MODEL
                + "'}}",
            Collection.V_NORR,
            success()),
        row(
            "R48",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'rerank': {}}",
            Collection.V_NORR,
            error(RequestException.Code.INVALID_RERANK_OVERRIDE)),
        row(
            "G11",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerank': {'provider': 'unknown', 'modelName': 'some-model'}}",
            Collection.VZ_RR,
            error(RequestException.Code.INVALID_RERANK_OVERRIDE)),

        // ---- order of errors ----
        row(
            "O02",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q'}",
            Collection.V_NORR,
            error(RequestException.Code.UNSUPPORTED_RERANKING_COMMAND)),
        row(
            "O03",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 101}",
            Collection.V_NORR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$vector")),
        row(
            "O04",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.VZ_RR,
            error(SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "O05",
            "{'$hybrid': {'$vectorize': 'cheese'}}",
            "{'rerankQuery': 'q', 'hybridLimits': 0}",
            Collection.V_LX_RR,
            error(SortException.Code.UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION)),
        row(
            "O07",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.PLAIN_NORR,
            error(SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION)),
        row(
            "O08",
            "{'$hybrid': {'$lexical': 'cows'}}",
            Q_ON,
            Collection.PLAIN_NORR,
            error(SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION)),
        row(
            "O09",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q', 'rerankOn': 'body', 'hybridLimits': 0, 'rerank': {}}",
            Collection.V_LX_RR,
            error(RequestException.Code.COMMAND_FIELD_VALUE_INVALID, "hybridLimits.$vector")),
        row(
            "O10",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerank': {}}",
            Collection.V_LX_RR,
            error(RequestException.Code.INVALID_RERANK_OVERRIDE)),
        row(
            "O11",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            NO_OPTIONS,
            Collection.V_LX_RR_EOL_MODEL,
            error(SchemaException.Code.END_OF_LIFE_AI_MODEL)),
        row(
            "O12",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'hybridLimits': 20}",
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_QUERY_TEXT)),
        row(
            "O13",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            "{'rerankQuery': 'q'}",
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_ON)),
        row(
            "O14",
            "{'$hybrid': {'$vector': " + VEC + ", '$lexical': 'cows'}}",
            Q_ON,
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            error(ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM)),
        row(
            "O15",
            "{'$hybrid': ''}",
            Q_ON,
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            // changed by the leg refactor (was UNSUPPORTED_PROJECTION_PARAM):
            // MISSING_HYBRID_SORT_VALUE now comes before the projection error
            error(RequestException.Code.MISSING_HYBRID_SORT_VALUE)),
        row(
            "O16",
            "{'$hybrid': {'$lexical': 'cows'}}",
            Q_ON,
            null,
            BAD_PROJECTION,
            Collection.LX_RR,
            error(ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM)),
        row(
            "G21",
            "{'$hybrid': ''}",
            "{'rerankQuery': 'q'}",
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            error(RequestException.Code.MISSING_RERANK_ON)),
        // an invalid rerankOn path fails after the rerank query check, before the projection error
        // and before the no $hybrid value error
        row(
            "O17",
            "{'$hybrid': {'$vector': " + VEC + "}}",
            "{'rerankQuery': 'q', 'rerankOn': 'a&b'}",
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            error(UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH)),
        row(
            "O18",
            "{'$hybrid': ''}",
            "{'rerankQuery': 'q', 'rerankOn': 'a&b'}",
            null,
            BAD_PROJECTION,
            Collection.V_LX_RR,
            error(UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PATH)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("rows")
  void buildOutcome(Row row) throws Exception {
    var commandContext = commandContext(row.collection());
    var command = objectMapper.readValue(row.commandJson(), FindAndRerankCommand.class);

    switch (row.expected()) {
      case Success success -> {
        var operation = builder(commandContext, command).build();
        assertThat(operation).as(row.id()).isNotNull();
        assertThat(commandContext.getHybridLimits().vectorLimit())
            .as(row.id() + " hybridLimits.$vector on the command context")
            .isEqualTo(success.vectorLimit());
        assertThat(commandContext.getHybridLimits().lexicalLimit())
            .as(row.id() + " hybridLimits.$lexical on the command context")
            .isEqualTo(success.lexicalLimit());
      }
      case Throws error -> {
        var ex =
            assertThrowsExactly(
                error.exceptionClass(), () -> builder(commandContext, command).build(), row.id());
        assertThat(ex.code).as(row.id()).isEqualTo(error.code());
        if (error.messageFragment() != null) {
          assertThat(ex.getMessage()).as(row.id()).contains(error.messageFragment());
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
  // Rows
  // ===========================================================================================

  /**
   * One request shape against one collection config.
   *
   * @param sort the sort JSON, or null when the command has no sort key
   * @param options the options JSON, or null when the command has no options key
   * @param filter the filter JSON, or null when the command has no filter key
   * @param projection the projection JSON, or null when the command has no projection key
   */
  record Row(
      String id,
      String sort,
      String options,
      String filter,
      String projection,
      Collection collection,
      Expected expected) {

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
      return ("{'findAndRerank': {" + String.join(", ", members) + "}}").replace('\'', '"');
    }

    @Override
    public String toString() {
      return "%s: sort=%s, options=%s, filter=%s, projection=%s on %s -> %s"
          .formatted(id, sort, options, filter, projection, collection, expected);
    }
  }

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

  sealed interface Expected permits Success, Throws {}

  /** build() returns an operation, and the hybrid limits copied onto the command context. */
  record Success(int vectorLimit, int lexicalLimit) implements Expected {}

  /** build() throws an API exception with this code, and optionally this message fragment. */
  record Throws(Class<? extends APIException> exceptionClass, String code, String messageFragment)
      implements Expected {}

  private static Expected success() {
    var defaults = FindAndRerankCommand.HybridLimits.DEFAULT;
    return new Success(defaults.vectorLimit(), defaults.lexicalLimit());
  }

  private static Expected success(int vectorLimit, int lexicalLimit) {
    return new Success(vectorLimit, lexicalLimit);
  }

  private static Expected error(RequestException.Code code) {
    return new Throws(RequestException.class, code.name(), null);
  }

  private static Expected error(RequestException.Code code, String messageFragment) {
    return new Throws(RequestException.class, code.name(), messageFragment);
  }

  private static Expected error(SortException.Code code) {
    return new Throws(SortException.class, code.name(), null);
  }

  private static Expected error(SchemaException.Code code) {
    return new Throws(SchemaException.class, code.name(), null);
  }

  private static Expected error(ProjectionException.Code code) {
    return new Throws(ProjectionException.class, code.name(), null);
  }

  private static Expected error(UpdateException.Code code) {
    return new Throws(UpdateException.class, code.name(), null);
  }

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  private CollectionSchemaObject schema(Collection collection) {
    return switch (collection) {
      case VZ_LX_RR -> vectorizeLexicalRerankSchema();
      case VZ_RR -> TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT;
      case V_LX_RR, V_LX_RR_EOL_MODEL ->
          TEST_CONSTANTS.VECTOR_LEXICAL_RERANK_COLLECTION_SCHEMA_OBJECT;
      case LX_RR -> TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT;
      case V_NORR -> TEST_CONSTANTS.VECTOR_COLLECTION_SCHEMA_OBJECT;
      case PLAIN_NORR -> TEST_CONSTANTS.COLLECTION_SCHEMA_OBJECT_LEGACY;
    };
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
                new CollectionRerankDef.RerankServiceDef("nvidia", RERANK_MODEL, null, null))));
  }

  /**
   * A real providers config (not a mock) so that rerank overrides are validated the same way as in
   * production, a mock has no providers and would reject every override.
   */
  private static RerankingProvidersConfig providersConfig(ApiModelSupport.SupportStatus status) {
    var model =
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
            RERANK_MODEL,
            new ApiModelSupport.ApiModelSupportImpl(status, Optional.empty()),
            false,
            "https://example.com/rerank",
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
                .RequestPropertiesImpl(3, 10, 100, 100, 0.5, 10));
    return new RerankingProvidersConfigImpl(
        Map.of(
            "nvidia",
            new RerankingProvidersConfigImpl.RerankingProviderConfigImpl(
                false, "nvidia", true, Map.of(), List.of(model))));
  }

  private CommandContext<CollectionSchemaObject> commandContext(Collection collection) {
    var schemaObject = schema(collection);
    var commandContext =
        TEST_CONSTANTS.collectionContext(CommandName.FIND_AND_RERANK, schemaObject);

    var modelStatus =
        collection == Collection.V_LX_RR_EOL_MODEL
            ? ApiModelSupport.SupportStatus.END_OF_LIFE
            : ApiModelSupport.SupportStatus.SUPPORTED;
    when(commandContext.rerankingProviderFactory().getRerankingConfig())
        .thenReturn(providersConfig(modelStatus));
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
}
