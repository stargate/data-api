package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsErrorWithStatus;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/**
 * findAndRerank tests of the sort clause: {@code $hybrid} as a string or an object, the
 * combinations of {@code $vectorize}, {@code $vector} and {@code $lexical}, and invalid values.
 *
 * <p>For review: JSON is written with single quotes ({@link #json}). A case name says what is sent;
 * it ends with "-> CODE" when it expects another code than the method comment, and with "(HCD)"
 * when it needs lexical search. In {@code status.trace}, "ANN 50" is the vector read and "BM25 50"
 * the lexical read; 50 is the read size main uses without {@code hybridLimits}. The full trace also
 * lists the inner read tasks and their find commands ({@code assertTracedTaskGroups}) and the
 * rerank step ({@code assertTracedRerank}); the docs do not cover tracing, so those checks pin
 * current behavior so that any change is visible in review.
 */
public interface FindAndRerankSortCases extends FindAndRerankTestContext {
  // Options of the sort-validation requests, so that only the sort can be at fault.
  String Q_AND_ON = "{'rerankQuery': 'q', 'rerankOn': 'title'}";
  // Selects m02 (150 pages) and m05 (120 pages) of the main collection.
  String PAIR_FILTER = "{'is_checked_out': false, 'number_of_pages': {'$lt': 151}}";
  String SNEAKERS = "ChatGPT integrated sneakers that talk to you";
  String NEW_DATA = "New data updated";
  String VECTOR_5 = "{'$hybrid': {'$vector': [0.1, 0.2, 0.3, 0.4, 0.5]}}";
  String ALL_NULL = "{'$hybrid': {'$vectorize': null, '$vector': null, '$lexical': null}}";
  String ONLY_LEXICAL = "{'$hybrid': {'$lexical': 'x'}}";
  String VECTOR_AND_LEXICAL =
      "{'$hybrid': {'$vector': [0.1, 0.2, 0.3, 0.4, 0.5], '$lexical': 'cheese milk cows'}}";
  String ORDER = "{'grp': 'order'}";
  String[] ORDER_TITLES = passages(BYO, "title", "o1", "o2", "o3", "o4");

  // Sends a sort value of the wrong JSON type and expects REQUEST_STRUCTURE_MISMATCH. Per the
  // findAndRerank docs (Parameters, sort), sort is an object, $vectorize and $lexical are strings,
  // and $vector is an array of numbers.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#wrongTypes")
  default void sortValueOfWrongTypeIsRejected(
      String sort, Fixture fixture, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request(sort, Q_AND_ON)), code, snippet);
  }

  static Stream<Arguments> wrongTypes() {
    String notObject = "sort clause must be an object or null";
    String binary = "(binary vector) contained fields with the wrong type. ";
    String notBinary = binary + wrongType("$vector", "ObjectNode");
    String extraKey = hybridField("$vector", "{'$binary': 'AAAAAA==', 'x': 1}");
    return Stream.of(
        mismatch("sort is 'cheese'", "'cheese'", notObject),
        mismatch("sort is [1]", "[1]", notObject),
        mismatch("sort is 1", "1", notObject),
        mismatch("sort is true", "true", notObject),
        wrongField("$vectorize is 1", "$vectorize", "1", "IntNode"),
        wrongField("$vectorize is true", "$vectorize", "true", "BooleanNode"),
        wrongField("$vectorize is ['x']", "$vectorize", "['x']", "ArrayNode"),
        wrongField("$vectorize is {'a': 'x'}", "$vectorize", "{'a': 'x'}", "ObjectNode"),
        wrongField("$lexical is 1", "$lexical", "1", "IntNode"),
        wrongField("$lexical is false", "$lexical", "false", "BooleanNode"),
        wrongField("$lexical is []", "$lexical", "[]", "ArrayNode"),
        wrongField("$lexical is {'a': 1}", "$lexical", "{'a': 1}", "ObjectNode"),
        wrongField("$vector is ''", "$vector", "''", "TextNode"),
        wrongField("$vector is 1", "$vector", "1", "IntNode"),
        wrongField("$vector is true", "$vector", "true", "BooleanNode"),
        badItem("$vector is [0.1, 'a', 0.3, 0.4, 0.5] -> SHRED_BAD_VECTOR_VALUE", "'a'", "String"),
        badItem("$vector is [0.1, null, 0.3, 0.4, 0.5] -> SHRED_BAD_VECTOR_VALUE", "null", "Null"),
        mismatch("$vector is {}", hybridField("$vector", "{}"), notBinary),
        mismatch("$vector is {'$date': 1}", hybridField("$vector", "{'$date': 1}"), notBinary),
        mismatch("$vector is {'$binary': 'AAAAAA==', 'x': 1}", extraKey, notBinary));
  }

  // Sends a sort value the docs do not cover and expects REQUEST_STRUCTURE_MISMATCH, or the code in
  // the case name (the main collection has dimension 5). The docs do not specify this; the test
  // pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#undocumented")
  default void sortValueTheDocsDoNotCoverIsRejected(
      String sort, Fixture fixture, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request(sort, Q_AND_ON)), code, snippet);
  }

  static Stream<Arguments> undocumented() {
    String unknownKey = "{'$hybrid': {'$vectorize': 'x', 'foo': 1}}";
    String wrongCase = "{'$hybrid': {'$Vectorize': 'x'}}";
    String three = "[0.1, 0.2, 0.3]";
    String dimension = "The configured vector dimension is: 5.";
    var size = DocumentException.Code.SHRED_BAD_VECTOR_SIZE;
    var length = DocumentException.Code.INVALID_VECTOR_LENGTH;
    return Stream.of(
        wrongField("$hybrid is null", "$hybrid", "null", "NullNode"),
        wrongField("$hybrid is ['a']", "$hybrid", "['a']", "ArrayNode"),
        wrongField("$hybrid is true", "$hybrid", "true", "BooleanNode"),
        mismatch("$hybrid is {'$vectorize': 'x', 'foo': 1}", unknownKey, "Unexpected fields: foo"),
        mismatch("$hybrid is {'$Vectorize': 'x'}", wrongCase, "Unexpected fields: $Vectorize"),
        vectorCase("$vector is [] -> SHRED_BAD_VECTOR_SIZE", "[]", size, "cannot be empty Array"),
        vectorCase(
            "$vector of 3 numbers, dimension 5 -> INVALID_VECTOR_LENGTH", three, length, dimension),
        args(
            "$vector, collection denies $vector from indexing -> SORT_CLAUSE_PATH_UNINDEXED",
            hybridField("$vector", three),
            NOVECTORIZE,
            SortException.Code.SORT_CLAUSE_PATH_UNINDEXED,
            "Collection field '$vector' is not indexed: cannot sort using it."));
  }

  // Sends sort shapes the design documents allow (a number $hybrid, a top-level $vector,
  // $vectorize, $lexical or plain field, a text $vector) and expects REQUEST_STRUCTURE_MISMATCH.
  // The findAndRerank docs (Parameters, sort) only show {"$hybrid": ...}. The test pins current
  // behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#designDocs")
  default void sortShapeFromTheDesignDocsIsRejected(
      String sort, Fixture fixture, ErrorCode<?> code, String snippet) {
    assertApiError(postToFixture(fixture, request(sort, Q_AND_ON)), code, snippet);
  }

  static Stream<Arguments> designDocs() {
    String only = "Expected fields: $hybrid. Unexpected fields: ";
    String vector = "{'$vector': [0.1, 0.2, 0.3, 0.4, 0.5]}";
    String both = "{'$hybrid': 'x', '$lexical': 'y'}";
    return Stream.of(
        wrongField("$hybrid is 1", "$hybrid", "1", "IntNode"),
        wrongField("$hybrid is 1.5", "$hybrid", "1.5", "DecimalNode"),
        mismatch("sort is {'$vector': [0.1, 0.2, 0.3, 0.4, 0.5]}", vector, only + "$vector"),
        mismatch("sort is {'$vectorize': 'x'}", "{'$vectorize': 'x'}", only + "$vectorize"),
        mismatch("sort is {'$lexical': 'x'}", "{'$lexical': 'x'}", only + "$lexical"),
        mismatch("sort is {'name': 1}", "{'name': 1}", only + "name"),
        mismatch("sort is {'$hybrid': 'x', '$lexical': 'y'}", both, only + "$lexical"),
        wrongField("$vector is 'I like cheese'", "$vector", "'I like cheese'", "TextNode"));
  }

  // Sends a sort with neither a vector nor a $vectorize text and expects UNEXPECTED_SERVER_ERROR
  // (its full message format is checked here once). The docs (Parameters, sort) do not cover this;
  // main fails with a server error (issue #2577). The test pins current behavior; whether it is a
  // bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#noSource")
  default void sortWithoutVectorSourceGivesServerError(
      String request, Fixture fixture, boolean needsLexical) {
    var response = postNeedingLexical(fixture, request, needsLexical);
    assertApiError(
        response,
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "An unexpected server error occurred while processing the request.",
        "Error Class: IllegalArgumentException\n",
        "Error Message: buildVectorRead() - no vector or vectorize\n",
        "Review the Error Message before retrying this request.");
    response.body("errors[0].title", is("Unexpected server error"));
  }

  static Stream<Arguments> noSource() {
    String nullVector = "{'$hybrid': {'$lexical': 'x', '$vector': null}}";
    String nullVectorize = "{'$hybrid': {'$lexical': 'x', '$vectorize': null}}";
    return Stream.of(
        mainCase("no sort", null, Q_AND_ON),
        mainCase("sort is null", "null", Q_AND_ON),
        mainCase("sort is an empty object", "{}", Q_AND_ON),
        mainCase("$hybrid is an empty string", "{'$hybrid': ''}", Q_AND_ON),
        mainCase("$hybrid is spaces", "{'$hybrid': '   '}", Q_AND_ON),
        mainCase("$hybrid is a space, a tab and a newline", "{'$hybrid': ' \\t\\n '}", Q_AND_ON),
        mainCase("$hybrid is an ideographic space U+3000", "{'$hybrid': '\\u3000'}", Q_AND_ON),
        mainCase("$hybrid is an empty object", "{'$hybrid': {}}", Q_AND_ON),
        mainCase("$hybrid with three nulls", ALL_NULL, Q_AND_ON),
        mainCase("$hybrid with a null $vectorize", "{'$hybrid': {'$vectorize': null}}", Q_AND_ON),
        mainCase("only $lexical (HCD)", ONLY_LEXICAL, Q_AND_ON),
        mainCase("$lexical, null $vector (HCD)", nullVector, Q_AND_ON),
        mainCase("$lexical, null $vectorize (HCD)", nullVectorize, Q_AND_ON),
        args("issue 2577 bad query 1 (HCD)", issue2577(true), MAIN, true));
  }

  // Sends a sort without a non-blank $vectorize text and no usable rerankQuery; expects
  // MISSING_RERANK_QUERY_TEXT, also without vector. Per the findAndRerank docs (Parameters, sort
  // and rerankQuery) it then needs a rerankQuery, which the brainstorm design doc's BYO-vector read
  // omits. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#noQuery")
  default void sortWithoutVectorizeTextNeedsRerankQuery(
      String request, Fixture fixture, boolean needsLexical) {
    var code = RequestException.Code.MISSING_RERANK_QUERY_TEXT;
    String missing = "command is missing the text to use as the query with the reranking";
    assertApiError(postNeedingLexical(fixture, request, needsLexical), code, missing);
  }

  static Stream<Arguments> noQuery() {
    String rerankOn = "{'rerankOn': 'title'}";
    String noVector = request("{'$hybrid': {}}", rerankOn);
    return Stream.of(
        mainCase("no sort", null, rerankOn),
        mainCase("sort is null", "null", rerankOn),
        mainCase("sort is an empty object", "{}", rerankOn),
        mainCase("$hybrid is blank", "{'$hybrid': '  '}", rerankOn),
        mainCase("$hybrid is an empty object", "{'$hybrid': {}}", rerankOn),
        mainCase("$hybrid with three nulls", ALL_NULL, rerankOn),
        args("no-vector collection, $hybrid is {}", noVector, NOVECTOR, false),
        mainCase("only $lexical (HCD)", ONLY_LEXICAL, rerankOn),
        mainCase("$vector and $lexical (HCD)", VECTOR_AND_LEXICAL, rerankOn),
        mainCase("$vector, rerankQuery is empty", VECTOR_5, "{'rerankQuery': ''}"),
        mainCase("$vector, rerankQuery is blank", VECTOR_5, "{'rerankQuery': '   '}"));
  }

  // Sends a sort without a $vectorize text, with a rerankQuery but no usable rerankOn, and expects
  // MISSING_RERANK_ON. The docs (Parameters, rerankOn) say it defaults to $lexical and is required
  // only with a $vector query. The test pins current behavior; whether it is a bug is still under
  // discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#noRerankOn")
  default void sortWithoutVectorizeTextNeedsRerankOn(
      String request, Fixture fixture, boolean needsLexical) {
    var code = RequestException.Code.MISSING_RERANK_ON;
    String missing = "command does not specify which document field to rerank on.";
    assertApiError(postNeedingLexical(fixture, request, needsLexical), code, missing);
  }

  static Stream<Arguments> noRerankOn() {
    String query = "{'rerankQuery': 'q'}";
    return Stream.of(
        mainCase("no sort", null, query),
        mainCase("$hybrid is an empty object", "{'$hybrid': {}}", query),
        mainCase("$vector, no rerankOn", VECTOR_5, query),
        mainCase("$vector, rerankOn is empty", VECTOR_5, "{'rerankQuery': 'q', 'rerankOn': ''}"),
        mainCase("$vector, rerankOn is blank", VECTOR_5, "{'rerankQuery': 'q', 'rerankOn': ' '}"),
        mainCase("only $lexical (HCD)", ONLY_LEXICAL, query),
        args("issue 2577 bad query 2 (HCD)", issue2577(false), MAIN, true));
  }

  // Sends a {"$binary": ...} $vector whose value is empty, not base64, not a whole number of floats
  // ('AAA=' is 2 bytes) or not a string, and expects a server error. The docs (Parameters, sort)
  // describe $vector only as an array of floats. The test pins current behavior; whether it is a
  // bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#badBinary")
  default void invalidBinaryVectorGivesServerError(
      String binary, ErrorCode<?> code, String snippet) {
    String sort = hybridField("$vector", "{'$binary': " + binary + "}");
    assertApiError(postToFixture(BYO, request(sort, Q_AND_ON)), code, snippet);
  }

  static Stream<Arguments> badBinary() {
    var internal = ServerException.Code.INTERNAL_SERVER_ERROR;
    var unexpected = ServerException.Code.UNEXPECTED_SERVER_ERROR;
    String badContent = "IllegalArgumentException\nError Message: Invalid content in EJSON $binary";
    String notWhole = "is not a multiple of 4 bytes long (2 bytes)";
    String nullBytes = "Error Class: NullPointerException";
    return Stream.of(
        args("$binary is '' -> INTERNAL_SERVER_ERROR", "''", internal, "Missing the vector value"),
        args("$binary is 'not base64!'", "'not base64!'", unexpected, badContent),
        args("$binary is 'AAA='", "'AAA='", unexpected, notWhole),
        args("$binary is 123", "123", unexpected, nullBytes),
        args("$binary is null", "null", unexpected, nullBytes));
  }

  // Sends a $vector array to a collection without vectorize, and expects rerank order, one vector
  // read and the vector echoed in status.sortVector, per the findAndRerank docs (Parameters, sort,
  // includeSortVector). The docs say reads default to limit (hybridLimits); main reads 50. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void vectorArraySortDoesOneVectorReadAndIsEchoed() {
    assertOrderGroupVectorSort("{'$vector': " + EXAMPLE_VECTOR + "}");
  }

  // Sends the same vector as {"$binary": <base64 of big-endian floats>}, or a null, empty or blank
  // $vectorize next to the $vector array, and expects the result of the plain array. The docs
  // (Parameters, sort) describe $vector as an array that cannot be used with $vectorize. The test
  // pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#lenient")
  default void vectorSortShapeOutsideTheDocsIsAccepted(String hybrid) {
    assertOrderGroupVectorSort(hybrid);
  }

  static Stream<Named<String>> lenient() {
    String vector = "'$vector': " + EXAMPLE_VECTOR;
    String binary = "{'$vector': {'$binary': '" + binaryOf(0.08f, -0.62f, 0.39f) + "'}}";
    return Stream.of(
        Named.of("$vector is a $binary of the same floats", binary),
        Named.of("$vectorize is null next to $vector", "{'$vectorize': null, " + vector + "}"),
        Named.of("$vectorize is empty next to $vector", "{'$vectorize': '', " + vector + "}"),
        Named.of("$vectorize is blank next to $vector", "{'$vectorize': '   ', " + vector + "}"));
  }

  // Sends a $vectorize text with a different $vector, and expects the $vectorize text to be
  // embedded and used as the rerank query. The docs (Parameters, sort) say the two cannot be used
  // together. The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void vectorizeAndVectorTogetherUsesVectorize() {
    String hybrid = "{'$vectorize': 'ChatGPT upgraded', '$vector': [0.9, 0.8, 0.7, 0.6, 0.5]}";
    assertPairRun(MAIN, hybrid, "ANN 50");
  }

  // Sends {"$vectorize": "  ChatGPT upgraded "} without $lexical, and expects the trimmed text as
  // embedding input and rerank query, and only the vector read. Per the findAndRerank docs
  // (Parameters, sort), $vectorize drives the vector search; the lexical search needs $lexical.
  @Test
  default void vectorizeTextIsTrimmedAndSkipsTheBm25Read() {
    assertPairRun(MAIN, "{'$vectorize': '  ChatGPT upgraded '}", "ANN 50");
  }

  // Sends {"$hybrid": "  ChatGPT upgraded  "}; expects the trimmed text as embedding input and
  // rerank query, and both reads on HCD, per the docs (Parameters, sort). On DSE the docs require
  // lexical for hybrid search, but main skips the BM25 read. The test pins current behavior;
  // whether it is a bug is still under discussion.
  @Test
  default void hybridStringIsTrimmedAndReadsFollowTheBackend() {
    String[] reads =
        lexicalAvailable() ? new String[] {"ANN 50", "BM25 50"} : new String[] {"ANN 50"};
    assertPairRun(MAIN, "'  ChatGPT upgraded  '", reads);
  }

  // Sends a $vectorize text with a null, empty or blank $lexical, or a null $vector, and expects
  // only the vector read, also on the collection without lexical. The docs do not specify this;
  // the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource("io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankSortCases#ignored")
  default void nullOrBlankSortValueIsIgnored(String extraKey, Fixture fixture) {
    assertPairRun(fixture, "{'$vectorize': 'ChatGPT upgraded', " + extraKey + "}", "ANN 50");
  }

  static Stream<Arguments> ignored() {
    return Stream.of(
        args("$lexical is null, collection with lexical on HCD", "'$lexical': null", MAIN),
        args("$lexical is empty, collection with lexical on HCD", "'$lexical': ''", MAIN),
        args("$lexical is blank, collection with lexical on HCD", "'$lexical': '   '", MAIN),
        args("$vector is null", "'$vector': null", MAIN),
        args("$lexical is null, collection without lexical", "'$lexical': null", NOLEX),
        args("$lexical is empty, collection without lexical", "'$lexical': ''", NOLEX),
        args("$lexical is blank, collection without lexical", "'$lexical': '   '", NOLEX));
  }

  // Sends {"$hybrid": "\u00a0"}, a no-break space that String.trim() keeps, with request tracing.
  // On HCD the BM25 read has no term and fails with INVALID_DATABASE_QUERY, and the trace shows
  // that the parallel vector read ran too; on DSE the character is the rerank query. No document
  // covers this. The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void noBreakSpaceHybridTextFailsOnTheBm25Read() {
    String request = request(PAIR_FILTER, "{'$hybrid': '\\u00a0'}", null);
    var response = postToFixture(MAIN, request, headers("Feature-Flag-request-tracing", "true"));
    if (lexicalAvailable()) {
      var code = DatabaseException.Code.INVALID_DATABASE_QUERY;
      response.statusCode(200).body("$", responseIsErrorWithStatus());
      assertSingleErrorAt(response, "errors", code, "BM25 query must contain at least one term");
      assertRerankerNotCalled();
      var reads = matches(response.extract().asString(), "Executing inner 'find' command");
      assertThat(reads).as("inner reads in status.trace").hasSize(2);
    } else {
      assertIds(response, "m05", "m02");
      assertRerankerCalls(calls(1).query("\u00a0").passages(SNEAKERS, NEW_DATA));
    }
  }

  // Sends different $vectorize and $lexical texts. On HCD each read uses its own trimmed text and
  // the rerank query is the $vectorize text; on DSE $lexical fails, as lexical is off. The
  // brainstorm design doc names the keys inconsistently; main uses $vectorize and $lexical. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void differentVectorizeAndLexicalTextsDriveTheirOwnReads() {
    String sort =
        "{'$hybrid': {'$vectorize': ' ChatGPT upgraded ', '$lexical': ' house hill grassy '}}";
    String options = "{'includeScores': true, 'includeSortVector': true}";
    var response = postToFixture(MAIN, request("{'grp': 'distinct'}", sort, options));
    if (!lexicalAvailable()) {
      var code = SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION;
      assertApiError(response, code, "The collection without a lexical index");
      return;
    }
    assertIds(response, "m05", "m03", "m02", "m04", "m01");
    assertScores(
        response,
        scores(3.25f, 0.93070626f, 3, 2, rrf(3, 2)),
        scores(2.125f, 0.8224976f, 4, 1, rrf(4, 1)),
        scores(1.5f, 0.9787127f, 1, 5, rrf(1, 5)),
        scores(0.5f, 0.9768599f, 2, 4, rrf(2, 4)),
        scores(-0.75f, 0.7665279f, 5, 3, rrf(5, 3)));
    String[] texts = passages(MAIN, "$vectorize", "m01", "m02", "m03", "m04", "m05");
    assertRerankerCalls(calls(1).query("ChatGPT upgraded").passages(texts));
    assertSortVector(response, 0.1, 0.16, 0.31, 0.22, 0.15);
  }

  // On HCD, sends a $vector array and a $lexical text with spaces around to the "order" documents
  // of the collection without vectorize, and expects both reads without an embedding call. Per the
  // findAndRerank docs (Parameters, sort), $vector and $lexical are the queries of the two reads.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void vectorAndLexicalSortDoesBothReadsWithoutEmbedding() {
    String hybrid = "{'$vector': " + EXAMPLE_VECTOR + ", '$lexical': '  house hill grassy '}";
    String options = "{'rerankQuery': 'q', 'rerankOn': 'title', 'includeScores': true}";
    String request = request(ORDER, hybridField("$hybrid", hybrid), options);
    var response = postToFixture(BYO, request, tracingHeaders());
    assertIds(response, "o4", "o3", "o2", "o1");
    assertScores(
        response,
        scores(3.0f, 0.8039639f, 3, 2, rrf(3, 2)),
        scores(2.0f, 0.9996167f, 1, 4, rrf(1, 4)),
        scores(0.75f, 0.5678263f, 4, 1, rrf(4, 1)),
        scores(-0.25f, 0.9535399f, 2, 3, rrf(2, 3)));
    assertRerankerCalls(calls(1).query("q").passages(ORDER_TITLES));
    String lexical = "0.0 true [$lexical]";
    String vector = "1.0 true [$vector]";
    var tasks = List.of(lexical, vector, lexical, vector);
    assertTracedTaskGroups(response, 2, tasks, "includeSimilarity=true, includeSortVector=false");
    assertTracedRerank(response, "q", "OPTIONS", "title", ORDER_TITLES);
  }

  // ---- helpers ----

  /**
   * Sends the $hybrid value to o1 to o4 of frr_byo and checks a single vector read, which sorts on
   * $vector from the start, no embedding task, and the traced rerank of the rerankQuery "q".
   */
  private void assertOrderGroupVectorSort(String hybrid) {
    String options = "{'rerankQuery': 'q', 'rerankOn': 'title', 'includeSortVector': true}";
    String request = request(ORDER, hybridField("$hybrid", hybrid), options);
    var response = postToFixture(BYO, request, tracingHeaders());
    assertIds(response, "o4", "o3", "o2", "o1");
    assertRerankerCalls(calls(1).query("q").passages(ORDER_TITLES));
    assertTracedReads(response, "ANN 50");
    assertSortVector(response, 0.08, -0.62, 0.39);
    var tasks = List.of("1.0 true [$vector]", "1.0 true [$vector]");
    assertTracedTaskGroups(response, 2, tasks, "includeSimilarity=false, includeSortVector=true");
    assertTracedRerank(response, "q", "OPTIONS", "title", ORDER_TITLES);
  }

  /**
   * Sends the $hybrid value to m05 and m02 of frr_main or n2 and n1 of frr_nolex, and checks the
   * order, the reranker request for "ChatGPT upgraded", the reads, the sortVector, and that the
   * text was embedded as a search query (request type SEARCH in status.trace), not as content. The
   * traced vector read has a deferred $vectorize and no sort until the embedding is done; the
   * traced rerank uses the trimmed sort text and the $vectorize passages.
   */
  private void assertPairRun(Fixture fixture, String hybrid, String... reads) {
    String filter = fixture == MAIN ? PAIR_FILTER : null;
    String request = request(filter, hybridField("$hybrid", hybrid), "{'includeSortVector': true}");
    var response = postToFixture(fixture, request, tracingHeaders());
    assertIds(response, fixture == MAIN ? new Object[] {"m05", "m02"} : new Object[] {"n2", "n1"});
    assertRerankerCalls(calls(1).query("ChatGPT upgraded").passages(SNEAKERS, NEW_DATA));
    assertTracedReads(response, reads);
    assertSortVector(response, 0.1, 0.16, 0.31, 0.22, 0.15);
    String type = "\"requestType\"\\s*:\\s*\"(\\w+)\"";
    assertThat(matches(response.extract().asString(), type))
        .as("embedding request types in status.trace")
        .isNotEmpty()
        .containsOnly("SEARCH");
    String lexical = "0.0 true [$lexical]";
    var tasks = List.of("1.0 false []", "1.0 false [$vector]");
    if (List.of(reads).contains("BM25 50")) {
      tasks = List.of(lexical, "1.0 false []", lexical, "1.0 false [$vector]");
    }
    assertTracedTaskGroups(response, 3, tasks, "includeSimilarity=false, includeSortVector=true");
    assertTracedRerank(response, "ChatGPT upgraded", "VECTORIZE", "$vectorize", SNEAKERS, NEW_DATA);
  }

  /**
   * The full trace has a start and a completion event for the outer group of composite tasks (an
   * embedding task when text is embedded, then the reads, then the rerank). Both list the inner
   * read tasks, the BM25 read at position 0 first, each here as "position deferredVectorize isNull
   * [sort paths]". Each read that runs also has an "Executing inner 'find' command" message whose
   * data is its find command, with these includes ("includeSimilarity=..., includeSortVector=...").
   */
  private static void assertTracedTaskGroups(
      ValidatableResponse response, int composites, List<String> readTasks, String includes) {
    String body = response.extract().asString();
    String start = "Starting to process taskGroup of CompositeTask with status={READY=%d}";
    String end = "Completed processing taskGroup of CompositeTask with status={COMPLETED=%d}";
    String event = "((?:Starting to process|Completed processing) taskGroup[^\"]*)";
    assertThat(matches(body, event))
        .as("task-group events in status.trace")
        .containsExactly(start.formatted(composites), end.formatted(composites));
    String task = "\"IntermediateCollectionReadTask\":\\{\"position\":([\\d.]+),.*?";
    String flag = "\"deferredVectorize isNull\":(true|false),";
    String paths = "\"sortClause\\.sortExpression\\.paths\":(\\[[^\\]]*\\])";
    var found = matches(body, task + flag + paths).stream().map(s -> s.replace("\"", ""));
    assertThat(found.toList())
        .as("inner read tasks in the task-group events of status.trace")
        .containsExactlyElementsOf(readTasks);
    // Filter and sort are written with object hashes, so only the projection and the options are
    // compared; every read gets the includeScores and includeSortVector of the findAndRerank.
    String inner =
        "status.trace.TraceSession.events.TraceEvent.findAll"
            + " { it.message.startsWith('Executing inner') }.data.RecordableMap.command";
    String command = "projectionDefinition=(.+), sortDefinition=.+, (options=Options\\[.+\\])\\]$";
    var commands = response.extract().jsonPath().getList(inner, String.class).stream();
    String options = "{\"*\":1} options=Options[limit=50, skip=0, pageState=null, %s]";
    var expected = Collections.nCopies(readTasks.size() / 2, options.formatted(includes));
    assertThat(commands.flatMap(c -> matches(c, command).stream()).toList())
        .as("find command data of the Executing inner messages in status.trace")
        .containsExactlyElementsOf(expected);
  }

  /**
   * The full trace of a rerank with the default limit 10 and the fake scores: the data of the
   * "Reranking N passages" message (query, limit, every passage sent), the scores of the
   * "Processing reranking response" message in the order of those passages, and the rerank task
   * group and task in the start and end task-group events (READY, then COMPLETED).
   */
  private static void assertTracedRerank(
      ValidatableResponse response, String query, String source, String locator, String... texts) {
    String body = response.extract().asString();
    String reranking = "Reranking " + texts.length + " passages";
    String processing = "Processing reranking response with " + texts.length + " scores";
    String using = " using NvidiaRerankingProvider with model " + Model.DEFAULT.modelName();
    assertThat(body).as("trace messages").contains(reranking + using, processing + using);
    var trace = response.extract().jsonPath();
    String event =
        "status.trace.TraceSession.events.TraceEvent.find { it.message.startsWith('%s') }";
    String sent = event.formatted(reranking) + ".data.RecordableMap";
    assertThat(trace.<String, Object>getMap(sent))
        .as("data of the Reranking message")
        .containsOnlyKeys("query", "limit", "passages")
        .containsEntry("query", Map.of("RerankingQuery", Map.of("query", query, "source", source)));
    assertThat(trace.<Number>get(sent + ".limit").doubleValue()).as("limit").isEqualTo(10.0);
    List<String> order = trace.getList(sent + ".passages", String.class);
    assertThat(order).as("passages in the Reranking message").containsExactlyInAnyOrder(texts);
    String rank = "Rank[index=%d, score=%s]";
    var ranks =
        IntStream.range(0, order.size())
            .mapToObj(i -> rank.formatted(i, FakeRerankerScores.find(order.get(i))))
            .toList();
    String scores = event.formatted(processing) + ".data.RecordableMap.scores";
    assertThat(trace.getList(scores, String.class))
        .as("scores in the Processing message, one per passage in the order above")
        .containsExactlyElementsOf(ranks);
    String group =
        "\"taskType\":\"RerankingTask\",\"sequentialProcessing\":(\\w+),\"size\":([\\d.]+),"
            + "\"statusCount\":\"([^\"]*)\",\"tasks\":\\[\\{\"RerankingTask\":\\{"
            + "\"position\":([\\d.]+),\"status\":\"(\\w+)\"";
    assertThat(matches(body, group))
        .as("rerank task group in the task-group events")
        .containsExactly("true 1.0 {READY=1} 0.0 READY", "true 1.0 {COMPLETED=1} 0.0 COMPLETED");
    // The provider is written with its object hash, so only its class name is compared.
    String task =
        "\"failure\":null,\"rerankingProvider\":\"[\\w.]+\\.(\\w+)@\\w+\",\"query\":"
            + "\\{\"RerankingQuery\":\\{\"query\":\"([^\"]*)\",\"source\":\"(\\w+)\"\\}\\},"
            + "\"passageLocator\":\"([^\"]*)\",\"limit\":([\\w.]+)\\}";
    String fields = String.join(" ", "NvidiaRerankingProvider", query, source, locator, "10.0");
    assertThat(matches(body, task))
        .as("rerank task in the task-group events")
        .containsExactly(fields, fields);
  }

  /** Every match of the regex in the text, as its groups joined with spaces. */
  private static List<String> matches(String text, String regex) {
    return Pattern.compile(regex)
        .matcher(text)
        .results()
        .map(m -> IntStream.rangeClosed(1, m.groupCount()).mapToObj(m::group))
        .map(groups -> groups.collect(Collectors.joining(" ")))
        .toList();
  }

  private ValidatableResponse postNeedingLexical(Fixture fixture, String request, boolean lexical) {
    if (lexical) {
      Assumptions.assumeTrue(lexicalAvailable(), "needs lexical search (HCD)");
    }
    return postToFixture(fixture, request);
  }

  /** Bad query 1 of issue #2577, or bad query 2, which has no rerankOn. */
  private static String issue2577(boolean withRerankOn) {
    String rerankOn = withRerankOn ? "'rerankOn': '$lexical', " : "";
    String options = "{" + rerankOn + "'rerankQuery': 'blo'}";
    return request("{'$lexical': {'$match': 'bla'}}", "{'$hybrid': {'$lexical': 'ble'}}", options);
  }

  /** A case on the main collection; a name ending with "(HCD)" needs lexical. */
  private static Arguments mainCase(String description, String sort, String options) {
    return args(description, request(sort, options), MAIN, description.endsWith("(HCD)"));
  }

  private static Arguments mismatch(String description, String sort, String snippet) {
    return args(description, sort, MAIN, RequestException.Code.REQUEST_STRUCTURE_MISMATCH, snippet);
  }

  private static Arguments wrongField(String description, String field, String value, String node) {
    return mismatch(description, hybridField(field, value), wrongType(field, node));
  }

  /** A SHRED_BAD_VECTOR_VALUE case: a 5-number $vector whose second item is this value. */
  private static Arguments badItem(String description, String item, String kind) {
    String vector = "[0.1, " + item + ", 0.3, 0.4, 0.5]";
    String snippet = "needs to be an array containing only Numbers but has a " + kind + " value";
    return vectorCase(description, vector, DocumentException.Code.SHRED_BAD_VECTOR_VALUE, snippet);
  }

  private static Arguments vectorCase(
      String description, String vector, ErrorCode<?> code, String snippet) {
    return args(description, hybridField("$vector", vector), MAIN, code, snippet);
  }

  /** {"$hybrid": <value>} for the field $hybrid, otherwise {"$hybrid": {<field>: <value>}}. */
  private static String hybridField(String field, String value) {
    String hybrid = field.equals("$hybrid") ? value : "{'" + field + "': " + value + "}";
    return "{'$hybrid': " + hybrid + "}";
  }

  private static String wrongType(String field, String node) {
    String allowed = field.equals("$hybrid") ? "TextNode, ObjectNode" : "NullNode, TextNode";
    allowed = field.equals("$vector") ? "NullNode, ArrayNode, ObjectNode" : allowed;
    return "Field %s may only be of types %s, but got: %s".formatted(field, allowed, node);
  }

  // ---- static helpers, also used by FindAndRerankCollectionCases ----

  /** A findAndRerank command from single-quoted JSON parts; a null part is left out. */
  static String request(String filter, String sort, String options) {
    var request = findAndRerank();
    Optional.ofNullable(filter).ifPresent(part -> request.filter(json(part)));
    Optional.ofNullable(sort).ifPresent(part -> request.sort(json(part)));
    Optional.ofNullable(options).ifPresent(part -> request.options(json(part)));
    return request.json();
  }

  static String request(String sort, String options) {
    return request(null, sort, options);
  }

  /** Arguments of a parameterized case; the first value is shown with the description. */
  static Arguments args(String description, Object first, Object... rest) {
    Object[] all = new Object[rest.length + 1];
    all[0] = Named.of(description, first);
    System.arraycopy(rest, 0, all, 1, rest.length);
    return Arguments.of(all);
  }

  static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }

  /** The default headers plus full request tracing, so that status.trace shows the read CQL. */
  static Map<String, Object> tracingHeaders() {
    return headers("Feature-Flag-request-tracing-full", "true");
  }

  /** The reads in status.trace, each as "ANN <limit>" or "BM25 <limit>", are exactly these. */
  static void assertTracedReads(ValidatableResponse response, String... reads) {
    String cql = "(ANN|BM25) OF \\? LIMIT (\\d+)";
    var found = new TreeSet<>(matches(response.extract().asString(), cql));
    assertThat(found).as("reads in status.trace").containsExactlyInAnyOrder(reads);
  }

  /** Base64 of the floats packed as big-endian 4-byte values, the $binary vector format. */
  static String binaryOf(float... values) {
    ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES);
    for (float value : values) {
      buffer.putFloat(value);
    }
    return Base64.getEncoder().encodeToString(buffer.array());
  }
}
