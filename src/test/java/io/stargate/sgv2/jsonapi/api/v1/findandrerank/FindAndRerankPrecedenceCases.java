package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.exception.FilterException.Code.FILTER_UNSUPPORTED_DATA_TYPE;
import static io.stargate.sgv2.jsonapi.exception.ProjectionException.Code.UNSUPPORTED_PROJECTION_DEFINITION;
import static io.stargate.sgv2.jsonapi.exception.RequestException.Code.*;
import static io.stargate.sgv2.jsonapi.exception.SchemaException.Code.*;
import static io.stargate.sgv2.jsonapi.exception.SortException.Code.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests about which error is reported when one request breaks several rules. Each
 * case expects only the error of the check that runs first; its description names the problems and
 * the error that wins. The checks run in this order: parsing the request (the first problem in the
 * JSON text wins, but unknown fields are reported after the known fields were read), bean
 * validation of the options, the collection lookup, building the command (sort support,
 * hybridLimits range, rerank service and override, query text, passage field, projection, then the
 * reads), and parsing the filter when the reads run. One exception: after the command, the server
 * parses the filter again for its metrics, and an unparsable filter's error replaces whatever error
 * the command returned.
 *
 * <p>The docs do not specify any of these orders. Every case also checks that the reranker was not
 * called. In the request bodies, single quotes become double quotes, and the upper-case names
 * listed at {@link #command} stand for longer JSON.
 */
public interface FindAndRerankPrecedenceCases extends FindAndRerankTestContext {

  String PRECEDENCE_FACTORIES =
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankPrecedenceCases#";

  // A collection name that no test creates.
  Fixture NO_SUCH_COLLECTION = collection("frr_no_such_collection", "{}");

  // Sends a request that breaks two or more rules and expects the single error of the check that
  // runs first. The docs do not specify this order; the test pins current behavior so that any
  // change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(PRECEDENCE_FACTORIES + "firstErrorCases")
  default void firstFailingCheckWins(Fixture on, String body, ErrorCode<?> code, String snippet) {
    String collection = on == NO_SUCH_COLLECTION ? on.name() : ensure(on);
    assertApiError(postToCollection(collection, command(body)), code, snippet);
  }

  static Stream<Arguments> firstErrorCases() {
    return Stream.of(
        // Found while the request is parsed and validated, before the collection lookup.
        when(NOVECTOR, "hybridLimits is a string, $vector sort, no vector -> parse error")
            .sends("{'sort': VECTOR_SORT, 'options': {'hybridLimits': 'x'}}")
            .fails(REQUEST_STRUCTURE_MISMATCH, "hybridLimits must be an integer or an object"),
        when(NO_SUCH_COLLECTION, "limit 0, collection does not exist -> limit error")
            .sends("{'sort': TEXT_SORT, 'options': {'limit': 0}}")
            .fails(COMMAND_FIELD_VALUE_INVALID, "'command.options.limit' value 0 not valid"),
        when(MAIN, "limit 0 and hybridLimits 0 -> only the limit error")
            .sends("{'sort': TEXT_SORT, 'options': {'limit': 0, 'hybridLimits': 0}}")
            .fails(COMMAND_FIELD_VALUE_INVALID, "'command.options.limit' value 0 not valid"),
        when(MAIN, "unknown field written before a sort that is a number -> sort error")
            .sends("{'foo': 1, 'sort': 5}")
            .fails(REQUEST_STRUCTURE_MISMATCH, "sort clause must be an object or null"),
        when(MAIN, "limit string written before sort string -> limit error")
            .sends("{'options': {'limit': 'abc'}, 'sort': 'x'}")
            .fails(REQUEST_STRUCTURE_MISMATCH, "`java.lang.Integer` from String \"abc\""),
        when(MAIN, "sort string written before limit string -> sort error")
            .sends("{'sort': 'x', 'options': {'limit': 'abc'}}")
            .fails(REQUEST_STRUCTURE_MISMATCH, "sort clause must be an object or null"),
        when(NO_SUCH_COLLECTION, "invalid JSON first, then bad sort, limit 0 -> REQUEST_NOT_JSON")
            .sends("{'filter': {'a': tru}, 'sort': 5, 'options': {'limit': 0}}")
            .fails(REQUEST_NOT_JSON, "Unrecognized token 'tru'"),
        when(NO_SUCH_COLLECTION, "limit 0, then bad sort, then invalid JSON -> sort error")
            .sends("{'options': {'limit': 0}, 'sort': 5, 'filter': {'a': tru}}")
            .fails(REQUEST_STRUCTURE_MISMATCH, "sort clause must be an object or null"),
        when(NO_SUCH_COLLECTION, "no rerankQuery or rerankOn, no collection -> collection error")
            .sends("{'sort': VECTOR_SORT}")
            .fails(UNKNOWN_COLLECTION_OR_TABLE, "frr_no_such_collection that does not exist"),
        // Found while the command is built.
        when(NOVECTOR, "no vector; $vectorize+$lexical, hybridLimits 0, unknown override -> vector")
            .sends("{'sort': BOTH_SORT, 'options': {'hybridLimits': 0, 'rerank': BAD_MODEL}}")
            .fails(UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION, "does not have vectors enabled"),
        when(NOVECTORIZE, "no vectorize; $vectorize+$lexical, unknown override -> vectorize")
            .sends("{'sort': BOTH_SORT, 'options': {'rerank': BAD_MODEL}}")
            .fails(UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION, "does not have vectorize enabled"),
        when(NOLEX, "no lexical; $vectorize+$lexical, unknown override -> lexical")
            .sends("{'sort': BOTH_SORT, 'options': {'rerank': BAD_MODEL}}")
            .fails(LEXICAL_NOT_ENABLED_FOR_COLLECTION, "The collection without a lexical index"),
        when(NORERANK, "hybridLimits 0, collection without rerank -> hybridLimits error")
            .sends("{'sort': TEXT_SORT, 'options': {'hybridLimits': 0}}")
            .fails(COMMAND_FIELD_VALUE_INVALID, "'hybridLimits.$vector' value 0 not valid"),
        when(NORERANK, "no vector source, collection without rerank -> rerank error")
            .sends("{'sort': {}, 'options': {'rerankQuery': 'q', 'rerankOn': 'title'}}")
            .fails(UNSUPPORTED_RERANKING_COMMAND, "a reranking service override was not provided"),
        when(MAIN, "hybridLimits 101 and unknown override model -> hybridLimits error")
            .sends("{'sort': TEXT_SORT, 'options': {'hybridLimits': 101, 'rerank': BAD_MODEL}}")
            .fails(COMMAND_FIELD_VALUE_INVALID, "'hybridLimits.$vector' value 101 not valid"),
        when(BYO, "unknown override model, no rerankQuery or rerankOn -> override error")
            .sends("{'sort': VECTOR_SORT, 'options': {'rerank': BAD_MODEL}}")
            .fails(INVALID_RERANK_OVERRIDE, "Model 'nvidia/no-such-model' is not supported"),
        when(MAIN, "END_OF_LIFE override model, no vector source -> model error")
            .sends("{'sort': {}, 'options': {'rerank': EOL_MODEL}}")
            .fails(END_OF_LIFE_AI_MODEL, "It is at END_OF_LIFE status"),
        when(BYO, "valid override, rerankOn but no rerankQuery -> query error")
            .sends("{'sort': VECTOR_SORT, 'options': {'rerankOn': 'title', 'rerank': GOOD_MODEL}}")
            .fails(MISSING_RERANK_QUERY_TEXT, "is missing the text to use as the query"),
        when(BYO, "$vector sort without rerankQuery and rerankOn -> query error")
            .sends("{'sort': VECTOR_SORT}")
            .fails(MISSING_RERANK_QUERY_TEXT, "is missing the text to use as the query"),
        when(BYO, "no rerankOn, projection is a string -> rerankOn error")
            .sends("{'projection': 'abc', 'sort': VECTOR_SORT, 'options': {'rerankQuery': 'q'}}")
            .fails(MISSING_RERANK_ON, "does not specify which document field to rerank on"),
        when(MAIN, "projection is a string, no vector source -> projection error")
            .sends(
                "{'projection': 'abc', 'sort': {}, 'options': {'rerankQuery': 'q', 'rerankOn':"
                    + " 'x'}}")
            .fails(UNSUPPORTED_PROJECTION_DEFINITION, "must be Object, was String"),
        // The filter is parsed again for the metrics, and its error replaces the build error.
        when(BYO, "filter and projection are strings, no rerankOn -> filter error")
            .sends(
                "{'filter': 'abc', 'projection': 'abc', 'sort': VECTOR_SORT, 'options':"
                    + " {'rerankQuery': 'q'}}")
            .fails(FILTER_UNSUPPORTED_DATA_TYPE, "must be JSON Object, command had String"),
        when(MAIN, "filter is a string, sort has no vector source -> filter error")
            .sends(
                "{'filter': 'abc', 'sort': {}, 'options': {'rerankQuery': 'q', 'rerankOn':"
                    + " 'title'}}")
            .fails(FILTER_UNSUPPORTED_DATA_TYPE, "must be JSON Object, command had String"));
  }

  // On HCD, a $lexical-only sort with rerankQuery, rerankOn and hybridLimits 0 gets the
  // hybridLimits error, not the server error such a sort gets when the reads are built. The docs do
  // not specify this order; the test pins current behavior so that any change is visible in review.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void hybridLimitsCheckedBeforeLexicalOnlySortFailsOnHcd() {
    String options = "{'hybridLimits': 0, 'rerankQuery': 'q', 'rerankOn': 'title'}";
    String body = "{'sort': {'$hybrid': {'$lexical': 'grassy'}}, 'options': " + options + "}";
    var snippet = "'hybridLimits.$vector' value 0 not valid";
    assertApiError(postToFixture(MAIN, command(body)), COMMAND_FIELD_VALUE_INVALID, snippet);
  }

  // Rewrites frr_comment's table comment so that its rerank model is END_OF_LIFE, then sends a
  // $vector sort with rerankQuery but no rerankOn: END_OF_LIFE_AI_MODEL, where the healthy
  // collection gives MISSING_RERANK_ON. The docs do not specify this order; the test pins current
  // behavior so that any change is visible in review. The comment is restored at the end.
  @Test
  default void eolCollectionModelReportedBeforeMissingRerankOn() {
    String collection = ensure(COMMENT);
    String sort = "{'$hybrid': {'$vector': [0.1, 0.2, 0.3, 0.4, 0.5]}}";
    String request = command("{'sort': " + sort + ", 'options': {'rerankQuery': 'q'}}");
    assertApiError(postToCollection(collection, request), MISSING_RERANK_ON, "field to rerank on");
    String eol = Model.EOL.modelName();
    TableCommentRewriter.withRewrittenComment(
        keyspace(),
        collection,
        c -> ((ObjectNode) c.at("/collection/options/rerank/service")).put("modelName", eol),
        () -> postToCollection(collection, request),
        TableCommentRewriter::errorCode,
        response -> assertApiError(response, END_OF_LIFE_AI_MODEL, "It is at END_OF_LIFE status"));
  }

  /** A case, written as {@code when(collection, description).sends(body).fails(code, snippet)}. */
  record PrecedenceCase(Fixture collection, String description, String body) {
    PrecedenceCase sends(String body) {
      return new PrecedenceCase(collection, description, body);
    }

    Arguments fails(ErrorCode<?> code, String snippet) {
      return Arguments.of(Named.of(description, collection), body, code, snippet);
    }
  }

  private static PrecedenceCase when(Fixture collection, String description) {
    return new PrecedenceCase(collection, description, null);
  }

  /**
   * {@code {"findAndRerank": body}}, with single quotes made double quotes and these names
   * replaced: VECTOR_SORT a $vector sort, TEXT_SORT a $hybrid string, BOTH_SORT a sort with
   * $vectorize and $lexical, BAD_MODEL an override with a model that nvidia does not have, and
   * EOL_MODEL and GOOD_MODEL overrides with the END_OF_LIFE and the default model.
   */
  private static String command(String body) {
    String override = "{'provider': 'nvidia', 'modelName': '%s'}";
    String json =
        body.replace("VECTOR_SORT", "{'$hybrid': {'$vector': [0.1, 0.2, 0.3]}}")
            .replace("TEXT_SORT", "{'$hybrid': 'cheese'}")
            .replace("BOTH_SORT", "{'$hybrid': {'$vectorize': 'cheese', '$lexical': 'cheese'}}")
            .replace("BAD_MODEL", override.formatted("nvidia/no-such-model"))
            .replace("EOL_MODEL", override.formatted(Model.EOL.modelName()))
            .replace("GOOD_MODEL", override.formatted(Model.DEFAULT.modelName()));
    return "{\"findAndRerank\": " + json.replace('\'', '"') + "}";
  }
}
