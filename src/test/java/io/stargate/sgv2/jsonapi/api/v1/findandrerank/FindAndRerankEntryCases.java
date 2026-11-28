package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.combine;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.mode;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.status;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;

import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Body;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Key;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.ExpectedRerankCalls;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.function.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/**
 * findAndRerank through the MCP tool, tables and other endpoints, and HTTP-level behavior: bodies
 * that cannot be parsed, headers, metrics, the command log and tracing. The docs describe neither
 * the MCP tool nor most of these errors, so most tests pin current behavior. MCP calls are JSON-RPC
 * POSTs to {@code /v1/mcp} without a session. Success cases mostly send "ChatGPT upgraded" to the
 * {@code grp: "distinct"} documents of {@code frr_main}: one reranker call with five passages,
 * result m05, m03, m02, m04, m01. Case JSON uses single quotes (see {@code json}). The metrics
 * tests compare /metrics before and after their requests, so tests must run one at a time.
 */
public interface FindAndRerankEntryCases extends FindAndRerankTestContext {

  String ENTRY_CASES = "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankEntryCases#";
  String QUERY = "ChatGPT upgraded";
  String SORT = json("'sort': {'$hybrid': {'$vectorize': 'ChatGPT upgraded'}}");
  String DISTINCT_ARGS = json("'filter': {'grp': 'distinct'}, ") + SORT;
  String DISTINCT_REQUEST = command(DISTINCT_ARGS);
  String[] PASSAGES = passages(MAIN, "$vectorize", "m01", "m02", "m03", "m04", "m05");
  Object[] RERANKED = {"m05", "m03", "m02", "m04", "m01"};
  String TRACING = ApiFeature.REQUEST_TRACING.httpHeaderName();
  String TRACING_FULL = ApiFeature.REQUEST_TRACING_FULL.httpHeaderName();
  String TOKEN = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME;
  String KEYSPACE = "<keyspace>";
  String NO_QUERY = "MISSING_RERANK_QUERY_TEXT";
  String FRR_METRIC = "command=\"FindAndRerankCommand\"";
  String TIMER = "command_processor_process_seconds";

  // Calls the MCP tool without an MCP header (the configuration turns MCP on): isError false, empty
  // content and _meta, only documents in structuredContent; extra arguments and limit 0 pass. The
  // docs do not describe the MCP tool; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "mcpSuccessCases")
  default void mcpToolReturnsRerankedDocuments(
      Fixture fixture, String arguments, Object[] ids, ExpectedRerankCalls rerankerCalls) {
    var response = assertMcpDocuments(callMcpTool(fixture, defaultHeaders(), arguments), ids);
    assertThat(response.extract().<Map<String, Object>>path("result._meta")).isEmpty();
    assertRerankerCalls(rerankerCalls);
  }

  static Stream<Arguments> mcpSuccessCases() {
    String vector = "'sort': {'$hybrid': {'$vector': " + EXAMPLE_VECTOR + "}}, 'options': ";
    String old = json(vector + "{'rerankQuery': 'I like cheese', 'rerankOn': 'content'}");
    String limit0 = args(", 'options': {'limit': 0}");
    Object[] none = {};
    return Stream.of(
        testCase("filter and sort -> rerank order", MAIN, DISTINCT_ARGS, RERANKED, five(1)),
        testCase("extra argument foo -> ignored", MAIN, args(", 'foo': 1"), RERANKED, five(1)),
        testCase("top-level limit 1 -> ignored", MAIN, args(", 'limit': 1"), RERANKED, five(1)),
        testCase("options.limit 0 -> reranked, no documents", MAIN, limit0, none, five(1)),
        testCase("existing MCP test call -> no passage, no reranking", BYO, old, none, calls(0)));
  }

  // Sends one request over HTTP and MCP with a projection, scores, sort vector, reranking-api-key
  // and Feature-Flag-reranking: false: MCP has the HTTP data in structuredContent and the HTTP
  // status in _meta.status, both calls use the key, and the header cannot turn off configured
  // reranking. The docs do not describe the MCP tool; the test pins current behavior.
  @Test
  default void mcpAndHttpGiveSameResultWithSameHeaders() {
    String rerankingKey = HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME;
    var headers = headers(rerankingKey, "farr-key", ApiFeature.RERANKING.httpHeaderName(), "false");
    String withScores = "'options': {'includeScores': true, 'includeSortVector': true}";
    String options = args(", 'projection': {'title': 1}, " + withScores);
    var http = assertIds(postToFixture(MAIN, command(options), headers), RERANKED);
    assertThat(http.extract().jsonPath().<Map<String, Object>>getList("data.documents"))
        .allSatisfy(document -> assertThat(document).containsOnlyKeys("_id", "title"));
    // Only the vector read runs: vector ranks follow m02, m04, m05, m03, m01; $rrf = 1/(60+rank).
    assertScores(
        http,
        scores(3.25f, 0.93070626f, 3, null, 1f / 63),
        scores(2.125f, 0.8224976f, 4, null, 1f / 64),
        scores(1.5f, 0.9787127f, 1, null, 1f / 61),
        scores(0.5f, 0.9768599f, 2, null, 1f / 62),
        scores(-0.75f, 0.7665279f, 5, null, 1f / 65));
    var mcp = assertMcpDocuments(callMcpTool(MAIN, headers, options), RERANKED);
    assertThat(mcp.extract().<Object>path("result.structuredContent"))
        .isEqualTo(http.extract().path("data"));
    assertThat(mcp.extract().<Object>path("result._meta"))
        .isEqualTo(Map.of("status", http.extract().path("status")));
    assertRerankerCalls(five(2).rerankingApiKey("farr-key"));
  }

  // Calls the MCP tool with options.limit -1: nothing validates it, so the reranker is called and
  // then UNEXPECTED_SERVER_ERROR. The docs do not cover this; main returns a server error. The test
  // pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void mcpNegativeLimitGivesServerErrorAfterReranking() {
    assertMcpToolError(
        callMcpTool(MAIN, defaultHeaders(), args(", 'options': {'limit': -1}")),
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "Error Class: IllegalArgumentException",
        "fromIndex(0) > toIndex(-1)");
    assertRerankerCalls(five(1));
  }

  // Calls the MCP tool with a request the command rejects or an unknown user's Token: HTTP 200, a
  // tool error with one six-field error, also for UNAUTHENTICATED_REQUEST (401 over HTTP). The docs
  // do not describe the MCP tool; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "mcpCommandErrorCases")
  default void mcpCommandErrorsAreToolErrors(Send send, ErrorCode<?> code, String text) {
    assertMcpToolError(send.apply(this), code, text);
    assertRerankerNotCalled();
  }

  static Stream<Arguments> mcpCommandErrorCases() {
    var noQuery = RequestException.Code.MISSING_RERANK_QUERY_TEXT;
    var noRerank = RequestException.Code.UNSUPPORTED_RERANKING_COMMAND;
    var unauthenticated = APISecurityException.Code.UNAUTHENTICATED_REQUEST;
    Send unknownUser = c -> c.callMcpTool(MAIN, headers(TOKEN, unknownUserToken()), DISTINCT_ARGS);
    return Stream.of(
        onMcp("only keyspace and collection -> MISSING_RERANK_QUERY_TEXT", MAIN, "")
            .fails(noQuery, "is missing the text"),
        onMcp("rerank disabled, no override -> UNSUPPORTED_RERANKING_COMMAND", NORERANK, SORT)
            .fails(noRerank, "is not enabled"),
        send("unknown user -> UNAUTHENTICATED_REQUEST", unknownUser)
            .fails(unauthenticated, "Authentication failed"));
  }

  // Calls the MCP tool on a table: tool error UNSUPPORTED_TABLE_COMMAND. The hybrid search docs say
  // these features are only for collections, while a design draft planned them for tables; main
  // rejects them. The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  default void mcpOnTableGivesUnsupportedTableCommand() {
    assertMcpToolError(
        callMcpTool(TABLE, defaultHeaders(), json("'sort': {'$hybrid': 'text'}")),
        RequestException.Code.UNSUPPORTED_TABLE_COMMAND,
        "The unsupported command was: findAndRerank.");
    assertRerankerNotCalled();
  }

  // Calls the MCP tool with arguments the framework cannot convert or without a required one: one
  // text and no structuredContent, not a Data API error. The docs do not cover this; main returns a
  // framework error. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "mcpArgumentErrorCases")
  default void mcpFrameworkRejectsBadArguments(String arguments, String text) {
    ensure(MAIN);
    var response =
        callMcpToolWithArguments(defaultHeaders(), arguments.replace(KEYSPACE, keyspace()));
    response.statusCode(200).body("error", nullValue()).body("result.isError", is(true));
    response.body("result.content", hasSize(1)).body("result.content[0].type", is("text"));
    response.body("result.content[0].text", is(text)).body("result.structuredContent", nullValue());
    assertRerankerNotCalled();
  }

  static Stream<Arguments> mcpArgumentErrorCases() {
    String ks = json("'keyspace': '" + KEYSPACE + "', ");
    String coll = json("'collection': 'frr_main', ");
    String options = "{" + ks + coll + SORT + json(", 'options': ");
    String hybrid5 = "{" + ks + coll + json("'sort': {'$hybrid': 5}}");
    String toSort = "Invalid argument [sort] - unable to convert value";
    String toOptions = "Invalid argument [options] - unable to convert value";
    String missing = "Missing required argument: ";
    return Stream.of(
        testCase("sort $hybrid is a number", hybrid5, toSort),
        testCase("hybridLimits is a string", options + json("{'hybridLimits': 'x'}}"), toOptions),
        testCase("options has an unknown field", options + json("{'x': 1}}"), toOptions),
        testCase("keyspace missing", "{" + coll + SORT + "}", missing + "keyspace"),
        testCase("collection missing", "{" + ks + SORT + "}", missing + "collection"));
  }

  // Calls the MCP tool with a filter that is not valid: JSON-RPC error -32603 "Internal error", no
  // result, no Data API error code. The docs do not cover this; main returns a framework error. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "mcpInternalErrorCases")
  default void mcpFilterErrorIsJsonRpcInternalError(Fixture fixture, String arguments) {
    callMcpTool(fixture, defaultHeaders(), arguments)
        .statusCode(200)
        .body("result", nullValue())
        .body("error.code", is(-32603))
        .body("error.message", is("Internal error"));
    assertRerankerNotCalled();
  }

  static Stream<Arguments> mcpInternalErrorCases() {
    String string = json("'filter': 'abc', ");
    String operator = json("'filter': {'name': {'$foo': 1}}, ");
    String text = json("'sort': {'$hybrid': 'text'}");
    return Stream.of(
        testCase("collection, filter is a string", MAIN, string + SORT),
        testCase("collection, filter with unknown operator", MAIN, operator + SORT),
        testCase("table, filter is a string", TABLE, string + text),
        testCase("table, filter with unknown operator", TABLE, operator + text));
  }

  // Lists the MCP tools: one runs findAndRerank, whose input schema uses Java names (vectorizeSort,
  // rerankServiceOverride, vectorLimit) and requires only keyspace and collection; descriptions are
  // pinned too. The docs do not describe the MCP tool; the test pins current behavior.
  @Test
  default void mcpToolSchemaUsesJavaFieldNames() {
    JsonPath tools = mcp(defaultHeaders(), "tools/list", "{}").statusCode(200).extract().jsonPath();
    assertThat(tools.getList("result.tools.name", String.class))
        .filteredOn(name -> name.contains("AndRerank"))
        .containsExactly("findAndRerank");
    String tool = "result.tools.find { it.name == 'findAndRerank' }";
    assertThat(tools.getString(tool + ".description"))
        .isEqualTo(
            "Finds documents using using vector and lexical sorting, then reranks the results.");
    String schema = tool + ".inputSchema";
    assertThat(tools.getList(schema + ".required", String.class))
        .containsExactlyInAnyOrder("keyspace", "collection");
    String props = schema + ".properties";
    String options = "hybridLimits includeScores includeSortVector limit rerankOn rerankQuery";
    String limits = "vectorLimit lexicalLimit commandFeatures";
    assertKeys(tools, schema, "keyspace collection filter projection sort options");
    assertKeys(tools, props + ".sort", "vectorizeSort lexicalSort vectorSort commandFeatures");
    assertKeys(tools, props + ".options", options + " rerankServiceOverride");
    assertKeys(tools, props + ".options.properties.hybridLimits", limits);
    assertKeys(tools, props + ".filter", "filterClause json");
    assertThat(tools.getString(props + ".options.properties.rerankOn.description"))
        .isEqualTo(
            "Name of the Document Field that contains the passage to rerank on, defaults to"
                + " $vectorize when that is used for sorting, required when other sort is used.");
    assertRerankerNotCalled();
  }

  // Sends findAndRerank to a table with a vectorize column: UNSUPPORTED_TABLE_COMMAND before option
  // checks, embedding or reads, but an invalid filter gives its filter error. The hybrid search
  // docs say these features are only for collections, while a design draft planned them for
  // tables. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "tableCases")
  default void tableRejectsFindAndRerank(Send send, ErrorCode<?> code, String text) {
    assertApiError(send.apply(this), code, text);
  }

  static Stream<Arguments> tableCases() {
    var unsupported = RequestException.Code.UNSUPPORTED_TABLE_COMMAND;
    var dataType = FilterException.Code.FILTER_UNSUPPORTED_DATA_TYPE;
    var operator = FilterException.Code.FILTER_UNSUPPORTED_OPERATOR;
    String was = "The unsupported command was: findAndRerank.";
    String text = "'sort': {'$hybrid': 'text'}";
    String override = "'rerank': {'provider': 'unknown', 'modelName': 'm'}";
    String limits = text + ", 'options': {'hybridLimits': 1000, " + override + "}";
    String vector = "'sort': {'$hybrid': {'$vector': [0.3, 0.3, 0.3, 0.3, 0.3]}}";
    String vectorize = "'sort': {'$hybrid': {'$vectorize': 'text'}}";
    String validFilter = "'filter': {'name': 'x'}, " + text;
    String stringFilter = "'filter': 'abc', " + text;
    String badOperator = "'filter': {'name': {'$foo': 1}}, " + text;
    return Stream.of(
        onTable("$hybrid text sort -> UNSUPPORTED_TABLE_COMMAND", text).fails(unsupported, was),
        onTable("unknown override provider, hybridLimits 1000 -> UNSUPPORTED_TABLE_COMMAND", limits)
            .fails(unsupported, was),
        onTable("$vector sort without rerankQuery -> UNSUPPORTED_TABLE_COMMAND", vector)
            .fails(unsupported, was),
        onTable("$vectorize sort -> UNSUPPORTED_TABLE_COMMAND", vectorize).fails(unsupported, was),
        onTable("valid filter -> UNSUPPORTED_TABLE_COMMAND", validFilter).fails(unsupported, was),
        onTable("filter is a string -> FILTER_UNSUPPORTED_DATA_TYPE", stringFilter)
            .fails(dataType, "must be JSON Object"),
        onTable("unknown filter operator -> FILTER_UNSUPPORTED_OPERATOR", badOperator)
            .fails(operator, "operator '$foo'"));
  }

  // Requests that fail before any read (bad JSON, duplicate keys, a too-long number, an unparsable
  // command, the wrong endpoint, an unknown collection, traced but still without status): HTTP 200
  // and one error. The docs do not specify these errors; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "earlyRequestErrorCases")
  default void entryRequestErrorsBeforeAnyRead(Send send, ErrorCode<?> code, String text) {
    assertApiError(send.apply(this), code, text);
  }

  static Stream<Arguments> earlyRequestErrorCases() {
    var notJson = RequestException.Code.REQUEST_NOT_JSON;
    var mismatch = RequestException.Code.REQUEST_STRUCTURE_MISMATCH;
    var unknown = RequestException.Code.COMMAND_UNKNOWN;
    var tooLong = DocumentException.Code.SHRED_DOC_LIMIT_VIOLATION;
    var noCollection = SchemaException.Code.UNKNOWN_COLLECTION_OR_TABLE;
    String valid = json("{'findAndRerank': {'sort': {'$hybrid': 'cheese'}}}");
    String twoSorts = "{'findAndRerank': {'sort': {'$hybrid': 'a', '$hybrid': 'b'}}}";
    String twoOptions = "{'findAndRerank': {'options': {}, 'options': {}}}";
    String digits = "{'findAndRerank': {'options': {'limit': 1" + "0".repeat(100) + "}}}";
    String twoCommands = "{'findAndRerank': {}, 'find': {}}";
    String asBoolean = "{'findAndRerank': true}";
    Send unknownCollection =
        c -> c.postToCollection("frr_no_such_collection", valid, headers(TRACING, "true"));
    return Stream.of(
        onMain("body is not valid JSON -> REQUEST_NOT_JSON", "{'findAndRerank': {'sort': }}")
            .fails(notJson, "problem:"),
        onMain("$hybrid twice in sort -> REQUEST_NOT_JSON", twoSorts)
            .fails(notJson, "Duplicate field '$hybrid'"),
        onMain("options twice -> REQUEST_NOT_JSON", twoOptions)
            .fails(notJson, "Duplicate field 'options'"),
        onMain("number with 101 digits -> SHRED_DOC_LIMIT_VIOLATION", digits)
            .fails(tooLong, "Number value length (101) exceeds"),
        onMain("name findAndRerankk -> COMMAND_UNKNOWN", "{'findAndRerankk': {}}")
            .fails(unknown, "'findAndRerankk' is not"),
        onMain("command value null -> REQUEST_STRUCTURE_MISMATCH", "{'findAndRerank': null}")
            .fails(mismatch, "from Null value"),
        onMain("command value is an array -> REQUEST_STRUCTURE_MISMATCH", "{'findAndRerank': []}")
            .fails(mismatch, "Array value"),
        onMain("command value is a string -> REQUEST_STRUCTURE_MISMATCH", "{'findAndRerank': 'x'}")
            .fails(mismatch, "String value"),
        onMain("command value is a number -> REQUEST_STRUCTURE_MISMATCH", "{'findAndRerank': 1}")
            .fails(mismatch, "Number value"),
        onMain("command value is a boolean -> REQUEST_STRUCTURE_MISMATCH", asBoolean)
            .fails(mismatch, "boolean value"),
        onMain("second top-level command -> REQUEST_STRUCTURE_MISMATCH", twoCommands)
            .fails(mismatch, "expected closing END_OBJECT"),
        onTable("table, $hybrid 5 -> REQUEST_STRUCTURE_MISMATCH", "'sort': {'$hybrid': 5}")
            .fails(mismatch, "Request is valid JSON but has a structural mismatch"),
        send("keyspace endpoint -> COMMAND_UNKNOWN", c -> c.postToKeyspace(valid))
            .fails(unknown, "not a Keyspace Command"),
        send("database endpoint -> COMMAND_UNKNOWN", c -> c.postToDatabase(valid, headers()))
            .fails(unknown, "Command 'findAndRerank' is not a General Command"),
        send("unknown collection, traced -> UNKNOWN_COLLECTION_OR_TABLE", unknownCollection)
            .fails(noCollection, "that does not exist in the Keyspace"));
  }

  // Each case breaks the documented request format: no JSON Content-Type, no Token, no sort, or a
  // field other than filter, options, projection and sort (names are case sensitive). Per the
  // findAndRerank docs (Signature), a request is a POST with Token and "Content-Type:
  // application/json" and only those four fields; Parameters does not mark sort as optional.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "signatureErrorCases")
  default void requestsBreakingDocumentedSignatureAreRejected(
      Send send, ErrorCode<?> code, String text, int httpStatus) {
    assertApiError(send.apply(this), httpStatus, code, text);
  }

  static Stream<Arguments> signatureErrorCases() {
    var html = RequestException.Code.UNSUPPORTED_CONTENT_TYPE;
    var noToken = APISecurityException.Code.MISSING_AUTHENTICATION_TOKEN;
    var noQuery = RequestException.Code.MISSING_RERANK_QUERY_TEXT;
    var field = RequestException.Code.COMMAND_FIELD_UNKNOWN;
    String known = "not one of known fields ('filter', 'options', 'projection', 'sort')";
    Send asHtml =
        c -> http(c).contentType(ContentType.HTML).body(DISTINCT_REQUEST).post(path(c)).then();
    Send withoutToken = c -> c.postToFixture(MAIN, DISTINCT_REQUEST, headers(TOKEN, null));
    return Stream.of(
        send("Content-Type text/html -> UNSUPPORTED_CONTENT_TYPE", asHtml)
            .fails(html, "Request sent with unsupported 'Content-Type' header value", 415),
        send("no Token header -> MISSING_AUTHENTICATION_TOKEN", withoutToken)
            .fails(noToken, "authorization token", 401),
        onMain("no sort -> MISSING_RERANK_QUERY_TEXT", "{'findAndRerank': {}}")
            .fails(noQuery, "is missing the text", 200),
        onMain("field foo -> COMMAND_FIELD_UNKNOWN", "{'findAndRerank': {'foo': 1}}")
            .fails(field, known, 200),
        onMain("top-level limit -> COMMAND_FIELD_UNKNOWN", "{'findAndRerank': {'limit': 5}}")
            .fails(field, "Command field 'limit' not recognized", 200),
        onMain("field Sort -> COMMAND_FIELD_UNKNOWN", "{'findAndRerank': {'Sort': {}}}")
            .fails(field, "Command field 'Sort' not recognized", 200));
  }

  // Sends the command as "FindAndRerank" and "findAndReRank": COMMAND_UNKNOWN naming it as sent.
  // The docs only write "findAndRerank", a design draft also "findAndReRank"; main accepts only the
  // exact name. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "commandNameCases")
  default void commandNameMustMatchExactly(String name) {
    assertApiError(
        postToFixture(MAIN, DISTINCT_REQUEST.replace("findAndRerank", name)),
        RequestException.Code.COMMAND_UNKNOWN,
        "Command '" + name + "' is not a Collection Command recognized by Data API.");
  }

  static Stream<Named<String>> commandNameCases() {
    return Stream.of(
        Named.of("FindAndRerank -> COMMAND_UNKNOWN", "FindAndRerank"),
        Named.of("findAndReRank of the design draft -> COMMAND_UNKNOWN", "findAndReRank"));
  }

  // A POST below the collection path and a GET on it get 404 with an empty body (no resource method
  // matches, so not 405). The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "wrongPathOrMethodCases")
  default void wrongPathOrMethodGetsEmptyResponse(Send send) {
    assertThat(send.apply(this).statusCode(404).extract().asString()).isEmpty();
    assertRerankerNotCalled();
  }

  static Stream<Named<Send>> wrongPathOrMethodCases() {
    Send below = c -> http(c).body(DISTINCT_REQUEST).post(path(c) + "/x").then();
    return Stream.of(
        Named.of("POST below the collection path -> 404", below),
        Named.<Send>of("GET on the collection path -> 404", c -> http(c).get(path(c)).then()));
  }

  // A 21 MB body gets 413 from the HTTP layer (20M limit), not a Data API error; "Expect:
  // 100-continue" lets the server answer before the body is sent. The docs do not specify this; the
  // test pins current behavior.
  @Test
  default void bodyOver20MegabytesGets413() throws IOException, InterruptedException {
    String body = command(json("'sort': {'$hybrid': '") + "x".repeat(21 * 1024 * 1024) + "\"}");
    var request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port() + path(this)))
            .expectContinue(true)
            .timeout(Duration.ofSeconds(60))
            .headers(TOKEN, dataApiToken(), "Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    var client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    var response = client.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(413);
    assertThat(response.body()).doesNotContain("errorCode");
    assertRerankerNotCalled();
  }

  // "forceSchemaRefresh": true at the top level is ignored, unlike other unknown fields, and the
  // request succeeds. The docs do not specify this; the test pins current behavior.
  @Test
  default void forceSchemaRefreshFieldIsIgnored() {
    assertIds(postToFixture(MAIN, command(args(", 'forceSchemaRefresh': true"))), RERANKED);
    assertRerankerCalls(five(1));
  }

  // Five requests: a success with limit 1, a filter that matches nothing, a failure before
  // reranking, a reranker call that waits 300 ms then fails with HTTP 500, and a blank
  // reranking-api-key, rejected in the provider before sending. The command timer counts each once
  // (failures by error code, all sort_type="NONE"); the passage metrics count the three requests
  // that reach the provider with all 5 passages; both call timers count the two sent calls and add
  // the same time, between the 300 ms wait and the elapsed time; only failures are command-logged.
  // The docs do not specify this; the test pins current behavior.
  @Test
  default void metricsAndCommandLogRecordFindAndRerank() throws IOException {
    var serverError = SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR;
    var noKey = SchemaException.Code.RERANKING_PROVIDER_AUTHENTICATION_KEY_NOT_PROVIDED;
    String count = TIMER + "_count";
    String slowFailure = combine(status(500, Body.JSON_MESSAGE), mode(Key.DELAY_MS, 300));
    FakeRerankerClient.stubMode(slowFailure);
    String rerankQuery = ", 'options': {'rerankQuery': '" + slowFailure + "'}";
    Path log = Path.of("target", "quarkus.log");
    long logSize = Files.size(log);
    var before = metricLines();
    long start = System.nanoTime();
    assertIds(postToFixture(MAIN, command(args(", 'options': {'limit': 1}"))), "m05");
    assertRerankerCalls(five(1));
    assertIds(postToFixture(MAIN, command(json("'filter': {'grp': 'none'}, ") + SORT)));
    postToFixture(MAIN, command("")).body("errors[0].errorCode", is(NO_QUERY));
    // The count includes the reranker request of the first success.
    var failedCall = postToFixture(MAIN, command(args(rerankQuery)));
    assertRuntimeError(failedCall, serverError, 2, "Provider: nvidia; HTTP Status: 500");
    var blankKey = postToFixture(MAIN, DISTINCT_REQUEST, headers("reranking-api-key", "   "));
    assertRuntimeError(blankKey, noKey, 2, "No authentication key provided for reranking provider");
    double seconds = (System.nanoTime() - start) / 1e9;
    var after = metricLines();
    String error = "error_code=\"REQUEST_";
    assertGrew(2, before, after, count, FRR_METRIC, "error=\"false\"");
    assertGrew(1, before, after, count, FRR_METRIC, error + NO_QUERY + "\"", features(""));
    assertGrew(1, before, after, count, FRR_METRIC, error + "SCHEMA_" + serverError + "\"");
    assertGrew(1, before, after, count, FRR_METRIC, error + "SCHEMA_" + noKey + "\"");
    assertThat(after)
        .filteredOn(line -> line.startsWith(TIMER) && line.contains(FRR_METRIC))
        .isNotEmpty()
        .allMatch(line -> line.contains("sort_type=\"NONE\""));
    String model = "reranking_model=\"" + Model.DEFAULT.modelName() + "\"";
    String[] all = {"reranking_provider=\"NvidiaRerankingProvider\"", model};
    String table = "table=\"" + MAIN.name() + "\"";
    String[] perTable = {"tenant=\"SINGLE-TENANT\"", "keyspace=\"" + keyspace() + "\"", table};
    var tagsBySeries = Map.of("rerank_all_", all, "rerank_tenant_", perTable);
    var durations = new ArrayList<Double>();
    tagsBySeries.forEach(
        (rerank, tags) -> {
          assertGrew(3, before, after, rerank + "passage_count_count", tags);
          assertGrew(15, before, after, rerank + "passage_count_sum", tags);
          assertGrew(2, before, after, rerank + "call_duration_seconds_count", tags);
          String sum = rerank + "call_duration_seconds_sum";
          double took = metricSum(after, sum, tags) - metricSum(before, sum, tags);
          assertThat(took).as(sum).isBetween(0.3, seconds);
          durations.add(took);
        });
    // Both timers record one measured time; the series started from different totals.
    assertThat(durations.get(0)).isCloseTo(durations.get(1), within(1e-6));
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () ->
                assertThat(logLines(log, logSize))
                    .anyMatch(l -> l.contains(NO_QUERY))
                    .anyMatch(l -> l.contains(serverError.name()))
                    .anyMatch(l -> l.contains(noKey.name())));
    assertThat(logLines(log, logSize)).hasSize(3);
  }

  // Sends a findAndRerank (the distinct group) or an insertOne (a "tmp" document, deleted again) to
  // frr_main: the command's error="false" timer grows by one, all in the series whose seven
  // feature_* tags are "true" exactly for the listed features. $hybrid always sets hybrid_string;
  // in a sort a key set to null still counts, in a document only non-null $hybrid subfields and a
  // non-null top-level $vector or $vectorize do. The $vector array and $binary cases rerank on
  // $vectorize with a rerankQuery, so the reranker request is unchanged. The docs do not specify
  // metrics; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "featureTagCases")
  default void commandTimerTagsTheFeaturesOfTheRequest(
      String command, String trueFeatures, TestInfo test) {
    boolean needsLexical = test.getDisplayName().endsWith("(HCD)");
    Assumptions.assumeTrue(lexicalAvailable() || !needsLexical, "needs lexical (HCD)");
    boolean insert = command.startsWith(json("{'insertOne'"));
    String commandTag = insert ? "command=\"InsertOneCommand\"" : FRR_METRIC;
    var before = metricLines();
    try {
      var response = postToFixture(MAIN, command);
      if (insert) {
        response.statusCode(200).body("errors", nullValue());
        response.body("status.insertedIds", contains("t-feature"));
      } else {
        assertIds(response, RERANKED);
      }
      var after = metricLines();
      String count = TIMER + "_count";
      String ok = "error=\"false\"";
      assertGrew(1, before, after, count, commandTag, ok);
      assertGrew(1, before, after, count, commandTag, ok, features(trueFeatures));
    } finally {
      if (insert) {
        postToFixture(MAIN, json("{'deleteMany': {'filter': {'grp': 'tmp'}}}")).statusCode(200);
      }
    }
    assertRerankerCalls(insert ? calls(0) : five(1));
  }

  static Stream<Arguments> featureTagCases() {
    String hybrid = json("'filter': {'grp': 'distinct'}, 'sort': {'$hybrid': ");
    String text = command(hybrid + json("'ChatGPT upgraded'}"));
    String lexNull =
        command(hybrid + json("{'$vectorize': 'ChatGPT upgraded', '$lexical': null}}"));
    String limit30 = command(args(", 'options': {'hybridLimits': 30}"));
    String limits = command(args(", 'options': {'hybridLimits': {'$vector': 80, '$lexical': 20}}"));
    // The test embedding provider's vector for "ChatGPT upgraded", as an array and as the Base64 of
    // the same five big-endian float32 values.
    String array = "{'$vectorize': null, '$vector': [0.1, 0.16, 0.31, 0.22, 0.15]";
    String binary = "{'$vector': {'$binary': 'PczMzT4j1wo+nrhSPmFHrj4ZmZo='}";
    String opts = "}}, 'options': {'rerankQuery': 'ChatGPT upgraded', 'rerankOn': '$vectorize'}";
    String nullVectorize = command(hybrid + json(array + opts));
    String binaryVector = command(hybrid + json(binary + opts));
    String nullVector =
        command(hybrid + json("{'$vectorize': 'ChatGPT upgraded', '$vector': null}}"));
    String hybridText = insertTmp("$hybrid", "'some text'");
    String both = insertTmp("$hybrid", "{'$lexical': 'some text', '$vectorize': 'some text'}");
    String hybridVec = insertTmp("$hybrid", "{'$vectorize': 'some text'}");
    String hybridNull = insertTmp("$hybrid", "null");
    String vector = insertTmp("$vector", "[0.1, 0.2, 0.3, 0.4, 0.5]");
    String vectorizeText = insertTmp("$vectorize", "'some text'");
    String hv = "hybrid_string vectorize";
    String hlv = "hybrid_string lexical vectorize";
    String hvv = "hybrid_string vector vectorize";
    return Stream.of(
        testCase("findAndRerank $hybrid text -> hybrid_string", text, "hybrid_string"),
        testCase("findAndRerank $vectorize -> hybrid_string, vectorize", DISTINCT_REQUEST, hv),
        testCase("findAndRerank null $lexical -> hybrid_string, lexical, vectorize", lexNull, hlv),
        testCase(
            "findAndRerank hybridLimits 30 -> also hybrid_limits_number",
            limit30,
            "hybrid_limits_number " + hv),
        testCase(
            "findAndRerank hybridLimits object -> also hybrid_limits_vector, hybrid_limits_lexical",
            limits,
            "hybrid_limits_lexical hybrid_limits_vector " + hv),
        testCase(
            "findAndRerank $vector array, null $vectorize -> hybrid_string, vector, vectorize",
            nullVectorize,
            hvv),
        testCase(
            "findAndRerank $binary $vector -> hybrid_string, vector",
            binaryVector,
            "hybrid_string vector"),
        testCase("findAndRerank null $vector -> hybrid_string, vector, vectorize", nullVector, hvv),
        testCase("insertOne $hybrid text -> hybrid_string (HCD)", hybridText, "hybrid_string"),
        testCase("insertOne $hybrid object -> hybrid_string, lexical, vectorize (HCD)", both, hlv),
        testCase("insertOne $hybrid $vectorize only -> hybrid_string, vectorize", hybridVec, hv),
        testCase("insertOne $hybrid null -> hybrid_string", hybridNull, "hybrid_string"),
        testCase("insertOne $vector array -> vector", vector, "vector"),
        testCase("insertOne top-level $vectorize -> vectorize", vectorizeText, "vectorize"),
        testCase("insertOne $vector null -> no feature", insertTmp("$vector", "null"), ""));
  }

  // Feature-Flag-request-tracing: true turns tracing on (the configuration leaves it unset): a
  // success has data and status.trace, an error has errors and status.trace, and MCP has
  // _meta.status.trace. The docs do not specify this; the test pins current behavior.
  @Test
  default void requestTracingHeaderAddsTrace() {
    var tracing = headers(TRACING, "true");
    assertIds(postToFixture(MAIN, DISTINCT_REQUEST, tracing), RERANKED)
        .body("status.trace", notNullValue());
    postToFixture(MAIN, command(""), tracing)
        .statusCode(200)
        .body("data", nullValue())
        .body("errors[0].errorCode", is(NO_QUERY))
        .body("status.trace", notNullValue());
    assertMcpDocuments(callMcpTool(MAIN, tracing, DISTINCT_ARGS), RERANKED)
        .body("result._meta.status.trace", notNullValue());
    assertRerankerCalls(five(2));
  }

  // Traces the distinct group plus m13 (a vector, no passage): one inner 'find' message per read
  // that runs, the de-duplication counts (m13 dropped, 5 documents left), then "Reranking 5
  // passages" and "Processing reranking response with 5 scores" naming the model, also with two
  // override batches. Only full tracing shows their data: query, limit, passages sent and one Rank
  // per passage. The docs do not specify the trace; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(ENTRY_CASES + "traceCases")
  default void traceShowsReadsDedupAndRerankModel(
      String sort, String tracing, int reads, int total, Model model, ExpectedRerankCalls calls) {
    Assumptions.assumeTrue(lexicalAvailable() || !sort.contains("$lexical"), "needs HCD");
    String filter = json("'filter': {'grp': {'$in': ['distinct', 'vec-only']}}, ");
    var response = postToFixture(MAIN, command(filter + sort), headers(tracing, "true"));
    assertIds(response, RERANKED);
    JsonPath traced = response.extract().jsonPath();
    String trace = traced.getString("status.trace");
    String read = "Executing inner 'find' command for schema object 'COLLECTION:%s.%s' ";
    String dedup = "reads=2, total documents=%d, dropped documents=1, deduplicated documents=5";
    String using = " using NvidiaRerankingProvider with model " + model.modelName();
    assertThat(trace.split(Pattern.quote(read.formatted(keyspace(), ensure(MAIN))), -1))
        .hasSize(reads + 1);
    assertThat(trace)
        .containsSubsequence(
            dedup.formatted(total),
            "Reranking 5 passages" + using,
            "Processing reranking response with 5 scores" + using);
    String data =
        "status.trace.TraceSession.events.TraceEvent.find { it.message.startsWith('%s') }.data";
    String sent = data.formatted("Reranking 5 passages");
    String scored = data.formatted("Processing reranking response");
    if (tracing.equals(TRACING)) {
      String omitted = "Data Omitted - for full data use header: " + TRACING_FULL + "=true";
      assertThat(traced.getString(sent)).isEqualTo(omitted);
      assertThat(traced.getString(scored)).isEqualTo(omitted);
    } else {
      var query = Map.of("RerankingQuery", Map.of("query", QUERY, "source", "VECTORIZE"));
      assertThat(traced.<String, Object>getMap(sent + ".RecordableMap"))
          .containsOnlyKeys("query", "limit", "passages")
          .containsEntry("query", query);
      assertThat(traced.getDouble(sent + ".RecordableMap.limit")).isEqualTo(10.0);
      // The passage order is internal, so Rank i is checked against the score of passage i; with
      // two batches this also shows the second batch's indexes continue from 3.
      var passages = traced.getList(sent + ".RecordableMap.passages", String.class);
      assertThat(passages).containsExactlyInAnyOrder(PASSAGES);
      String rank = "Rank[index=%d, score=%s]";
      var ranks =
          IntStream.range(0, passages.size())
              .mapToObj(i -> rank.formatted(i, FakeRerankerScores.find(passages.get(i))));
      assertThat(traced.getList(scored + ".RecordableMap.scores", String.class))
          .containsExactlyElementsOf(ranks.toList());
    }
    assertRerankerCalls(calls);
  }

  static Stream<Arguments> traceCases() {
    String both = json("'sort': {'$hybrid': {'$vectorize': '" + QUERY + "', '$lexical': 'house'}}");
    String model = "'modelName': '" + Model.SECOND.modelName() + "'";
    String override = SORT + json(", 'options': {'rerank': {'provider': 'nvidia', " + model + "}}");
    var second = calls(2).on(Model.SECOND).query(QUERY).passages(PASSAGES).batchSizes(3, 2);
    BiFunction<String, String, Arguments> overridden =
        (name, tracing) -> testCase(name, override, tracing, 1, 6, Model.SECOND, second);
    return Stream.of(
        testCase(
            "$vectorize and $lexical -> two reads", both, TRACING, 2, 11, Model.DEFAULT, five(1)),
        testCase("$vectorize only -> one read", SORT, TRACING, 1, 6, Model.DEFAULT, five(1)),
        overridden.apply("rerank override -> override model", TRACING),
        overridden.apply(
            "rerank override, full tracing -> query, limit, passages and scores", TRACING_FULL));
  }

  /** Turns JSON written with single quotes into JSON; the case JSON has no other single quotes. */
  private static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }

  private static String command(String members) {
    return "{\"findAndRerank\": {" + members + "}}";
  }

  private static String args(String moreMembers) {
    return DISTINCT_ARGS + json(moreMembers);
  }

  private static ExpectedRerankCalls five(int count) {
    var all = Collections.nCopies(count, PASSAGES).stream().flatMap(Arrays::stream);
    return calls(count).query(QUERY).passages(all.toArray(String[]::new));
  }

  /** The arguments of a case: the first value named with the case description, then the rest. */
  private static Arguments testCase(String name, Object first, Object... rest) {
    Stream<Object> values = Stream.concat(Stream.of(Named.of(name, first)), Arrays.stream(rest));
    return Arguments.of(values.toArray());
  }

  /** A request that a parameterized case sends from the running test. */
  interface Send extends Function<FindAndRerankEntryCases, ValidatableResponse> {}

  /** A named request whose name ends with " -> " and the error code it expects. */
  record ErrorCase(String name, Send send) {
    /** The case arguments: the request, the code, a message snippet, then {@code more} values. */
    Arguments fails(ErrorCode<?> code, String text, Object... more) {
      if (!name.endsWith(" -> " + code)) {
        throw new IllegalArgumentException("Case name must end with -> " + code + ": " + name);
      }
      var rest = Stream.concat(Stream.<Object>of(code, text), Arrays.stream(more)).toArray();
      return testCase(name, send, rest);
    }
  }

  private static ErrorCase send(String name, Send request) {
    return new ErrorCase(name, request);
  }

  private static ErrorCase onMain(String name, String body) {
    return send(name, c -> c.postToFixture(MAIN, json(body)));
  }

  private static ErrorCase onTable(String name, String members) {
    return send(name, c -> c.postToFixture(TABLE, command(json(members))));
  }

  private static ErrorCase onMcp(String name, Fixture fixture, String arguments) {
    return send(name, c -> c.callMcpTool(fixture, c.defaultHeaders(), arguments));
  }

  private static RequestSpecification http(FindAndRerankEntryCases c) {
    return given().port(c.port()).headers(c.defaultHeaders()).contentType(ContentType.JSON);
  }

  private static String path(FindAndRerankEntryCases c) {
    return "/v1/" + c.keyspace() + "/" + c.ensure(MAIN);
  }

  /** The test Token with the user name "invalid", a user that does not exist. */
  private static String unknownUserToken() {
    String password = dataApiToken().substring(dataApiToken().lastIndexOf(':'));
    return "Cassandra:" + Base64.getEncoder().encodeToString("invalid".getBytes(UTF_8)) + password;
  }

  /** Posts one JSON-RPC request to the MCP endpoint, without a session, accepting JSON back. */
  private ValidatableResponse mcp(Map<String, ?> headers, String method, String params) {
    String body = json("{'jsonrpc': '2.0', 'id': 1, 'method': '%s', 'params': %s}");
    var request = given().port(port()).headers(headers).contentType(ContentType.JSON);
    request.accept("application/json, text/event-stream");
    return request.body(body.formatted(method, params)).post("/v1/mcp").then();
  }

  private ValidatableResponse callMcpToolWithArguments(Map<String, ?> headers, String arguments) {
    String params = json("{'name': 'findAndRerank', 'arguments': ") + arguments + "}";
    return mcp(headers, "tools/call", params);
  }

  private ValidatableResponse callMcpTool(Fixture fixture, Map<String, ?> headers, String members) {
    String target = "{'keyspace': '" + keyspace() + "', 'collection': '" + ensure(fixture) + "'";
    String more = members.isEmpty() ? "}" : ", " + members + "}";
    return callMcpToolWithArguments(headers, json(target) + more);
  }

  /** Success: isError false, no content, and structuredContent with only these documents. */
  private static ValidatableResponse assertMcpDocuments(ValidatableResponse response, Object... i) {
    response.statusCode(200).body("error", nullValue()).body("result.isError", is(false));
    response.body("result.content", empty());
    assertThat(response.extract().<Map<String, Object>>path("result.structuredContent"))
        .containsOnlyKeys("documents", "nextPageState")
        .containsEntry("nextPageState", null);
    assertThat(response.extract().jsonPath().getList("result.structuredContent.documents._id"))
        .containsExactly(i);
    return response;
  }

  /** Tool error: HTTP 200, isError true, no content, empty _meta, one error with six fields. */
  private static void assertMcpToolError(
      ValidatableResponse response, ErrorCode<?> code, String... texts) {
    response.statusCode(200).body("error", nullValue()).body("result.isError", is(true));
    response.body("result.content", empty());
    assertThat(response.extract().<Map<String, Object>>path("result._meta")).isEmpty();
    assertThat(response.extract().<Map<String, Object>>path("result.structuredContent"))
        .containsOnlyKeys("errors");
    assertThat(response.extract().<Map<String, Object>>path("result.structuredContent.errors[0]"))
        .containsOnlyKeys("id", "family", "scope", "errorCode", "title", "message");
    assertSingleErrorAt(response, "result.structuredContent.errors", code, texts);
  }

  /** The property names of the JSON schema object at {@code path} are exactly these words. */
  private static void assertKeys(JsonPath json, String path, String names) {
    assertThat(json.getMap(path + ".properties").keySet())
        .as(path)
        .containsExactlyInAnyOrder((Object[]) names.split(" "));
  }

  private List<String> metricLines() {
    var metrics = given().port(port()).get("/metrics").then().statusCode(200).extract().asString();
    return metrics.lines().toList();
  }

  /** The sum of the values of the {@code series} lines of /metrics that contain every tag. */
  private static double metricSum(List<String> metrics, String series, String... tags) {
    return metrics.stream()
        .filter(line -> line.startsWith(series + "{") && Stream.of(tags).allMatch(line::contains))
        .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
        .sum();
  }

  /** That sum grew by exactly {@code expected} between the two reads of /metrics. */
  private static void assertGrew(
      double expected, List<String> before, List<String> after, String series, String... tags) {
    assertThat(metricSum(after, series, tags) - metricSum(before, series, tags))
        .as("growth of %s with %s", series, String.join(" ", tags))
        .isEqualTo(expected);
  }

  /** The seven feature_* tags as /metrics prints them (sorted), "true" for these names only. */
  private static String features(String trueFeatures) {
    var on = List.of(trueFeatures.split(" "));
    String names = "hybrid_limits_lexical hybrid_limits_number hybrid_limits_vector hybrid_string";
    var tags = Stream.of((names + " lexical vector vectorize").split(" "));
    return String.join(",", tags.map(n -> "feature_" + n + "=\"" + on.contains(n) + "\"").toList());
  }

  /** An insertOne of the document "t-feature" in the group "tmp" with one more field. */
  private static String insertTmp(String field, String value) {
    String document = "{'_id': 't-feature', 'grp': 'tmp', '" + field + "': " + value + "}";
    return json("{'insertOne': {'document': " + document + "}}");
  }

  private static List<String> logLines(Path log, long offset) throws IOException {
    byte[] all = Files.readAllBytes(log);
    var added = new String(all, (int) offset, all.length - (int) offset, UTF_8);
    return added.lines().filter(line -> line.contains("FindAndRerankCommand")).toList();
  }
}
