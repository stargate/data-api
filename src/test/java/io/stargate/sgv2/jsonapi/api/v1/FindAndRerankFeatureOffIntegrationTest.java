package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSingleErrorAt;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.CUSTOM_VECTORIZE_5;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.OFF_NORERANK;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.OFF_RERANK;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.OFF_TABLE;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.headers;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerNotCalled;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.calls;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankTestContext;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import io.stargate.sgv2.jsonapi.testresource.FeatureOffFakeRerankerTestResource;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Regression tests for findAndRerank with features off in the configuration ({@link
 * FeatureOffFakeRerankerTestResource}): the reranking and MCP flags are blank, so only a request
 * header {@code Feature-Flag-reranking: true} or {@code Feature-Flag-mcp: true} turns them on;
 * nvidia is disabled; hybridLimits allows 0 to 1500 for $vector and 0 to 1600 for $lexical; the
 * default limit is 1. Mind the schema cache: a rerank-enabled schema cannot load with the feature
 * off, so every command on it fails; once loaded with the feature on, only findAndRerank checks the
 * flag. DDL commands empty the cache, so {@link #cacheRerankCollectionSchema()} reloads
 * frr_off_rerank before each test; frr_off_uncached is never cached.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = FeatureOffFakeRerankerTestResource.class,
    restrictToAnnotatedClass = true)
public class FindAndRerankFeatureOffIntegrationTest extends AbstractCollectionIntegrationTestBase
    implements FindAndRerankTestContext {

  private static final String RERANKING = ApiFeature.RERANKING.httpHeaderName();
  private static final String MCP = ApiFeature.MCP.httpHeaderName();
  private static final String TRACING = ApiFeature.REQUEST_TRACING_FULL.httpHeaderName();
  private static final ErrorCode<?> FEATURE = SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED;
  private static final ErrorCode<?> UNKNOWN = SchemaException.Code.UNKNOWN_COLLECTION_OR_TABLE;
  private static final String MISSING = "frr_off_missing";
  private static final String COUNT = "{\"countDocuments\": {}}";
  private static final String QUERY = "ChatGPT upgraded";
  private static final String SORT = "{\"$hybrid\": {\"$vectorize\": \"" + QUERY + "\"}}";
  private static final String REQUEST = findAndRerank().sort(SORT).json();

  /** The $vectorize texts of the two documents of frr_off_rerank. */
  private static final Map<String, String> PASSAGES =
      Map.of("f1", "Updating new data", "f2", "A deep learning display that controls your mood");

  /** Same options as frr_off_rerank, no documents; no request with reranking on reads it. */
  private static final Fixture UNCACHED = created("frr_off_uncached", OFF_RERANK.hcdDefinition());

  private static final Fixture ENABLED_ONLY =
      created(
          "frr_off_enabled_only", "{" + CUSTOM_VECTORIZE_5 + ", \"rerank\": {\"enabled\": true}}");

  private final Set<String> createdFixtures = ConcurrentHashMap.newKeySet();

  /** No default collection: the tests create the fixtures they use on first use. */
  @BeforeAll
  @Override
  public final void createDefaultCollection() {}

  @Override
  public String keyspace() {
    return keyspaceName;
  }

  @Override
  public boolean lexicalAvailable() {
    return isLexicalAvailableForDB();
  }

  @Override
  public int port() {
    return getTestPort();
  }

  @Override
  public Set<String> createdFixtures() {
    return createdFixtures;
  }

  /** Creates the fixtures, caches the frr_off_rerank schema and checks that it is cached. */
  @BeforeEach
  void cacheRerankCollectionSchema() {
    List.of(OFF_RERANK, OFF_NORERANK, OFF_TABLE, UNCACHED).forEach(this::ensure);
    postToFixture(OFF_RERANK, COUNT, headers(RERANKING, "true")).body("status.count", is(2));
    postToFixture(OFF_RERANK, COUNT).body("errors", is(nullValue())).body("status.count", is(2));
  }

  // Sends findAndRerank with the reranking header (two spellings) and no limit. Expects one
  // request with both passages on the disabled default nvidia model, then only f2: the configured
  // default limit is 1, which the docs (Parameters, limit) leave to the Data API. The docs do not
  // specify a disabled default provider; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("featureHeaderNames")
  void featureHeaderTurnsRerankingOnAndDisabledProviderStillReranks(String headerName) {
    assertIds(postToFixture(OFF_RERANK, REQUEST, headers(headerName, "true")), "f2");
    assertRerankedOnce("f2", "f1");
  }

  static Stream<Named<String>> featureHeaderNames() {
    return Stream.of(
        Named.of("Feature-Flag-reranking: true -> reranks with the default model", RERANKING),
        Named.of("header name in lower case -> reranks", RERANKING.toLowerCase(Locale.ROOT)));
  }

  // Sends findAndRerank or countDocuments. Expects the error of the first failing check: request
  // validation, collection lookup, loading an uncached rerank schema, the reranking flag ("true"
  // only), the hybridLimits bounds ($vector 1501 is over 1500, though $lexical allows 1600), the
  // database ("hybridLimits": 0). The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("requestErrorCases")
  void requestErrorsComeFromTheFirstFailingCheck(
      String collection, String request, String header, ErrorCode<?> code, String message) {
    var response = postToCollection(collection, request, headers(RERANKING, header));
    assertApiError(response, code, message);
  }

  static Stream<Arguments> requestErrorCases() {
    String rerank = OFF_RERANK.name();
    String uncached = UNCACHED.name();
    String minimal = findAndRerank().hybrid(QUERY).json();
    String full =
        """
        {"findAndRerank": {"filter": {"_id": "f1"}, "projection": {"_id": 1},
          "sort": {"$hybrid": {"$vectorize": "ChatGPT upgraded", "$lexical": "orchard"}},
          "options": {"limit": 1, "hybridLimits": 5, "rerankOn": "$vectorize", "rerankQuery": "q",
            "includeScores": true, "includeSortVector": true}}}""";
    String empty = findAndRerank().sort(SORT).option("rerank", Map.of()).json();
    var tooHigh = Map.of("$vector", 1501, "$lexical", 10);
    String overMax = findAndRerank().sort(SORT).option("hybridLimits", tooHigh).json();
    String limit0 = findAndRerank().hybrid(QUERY).option("limit", 0).json();
    String zero = findAndRerank().sort(SORT).option("hybridLimits", 0).json();
    var invalid = RequestException.Code.COMMAND_FIELD_VALUE_INVALID;
    var database = DatabaseException.Code.INVALID_DATABASE_QUERY;
    String msg =
        "'hybridLimits.$vector' value 1501 not valid: must be between 0 and 1500 (inclusive)";
    return Stream.of(
        off("minimal request -> RERANKING_FEATURE_NOT_ENABLED", rerank, minimal, null),
        off("request with every part set -> RERANKING_FEATURE_NOT_ENABLED", rerank, full, null),
        off("empty rerank override -> RERANKING_FEATURE_NOT_ENABLED", rerank, empty, null),
        off("hybridLimits above 1500 -> RERANKING_FEATURE_NOT_ENABLED", rerank, overMax, null),
        off("rerank disabled -> RERANKING_FEATURE_NOT_ENABLED", OFF_NORERANK.name(), minimal, null),
        off("header value TRUE -> RERANKING_FEATURE_NOT_ENABLED", rerank, minimal, "TRUE"),
        off("header value 1 -> RERANKING_FEATURE_NOT_ENABLED", rerank, minimal, "1"),
        off("header value yes -> RERANKING_FEATURE_NOT_ENABLED", rerank, minimal, "yes"),
        off("uncached schema -> RERANKING_FEATURE_NOT_ENABLED", uncached, minimal, null),
        off("uncached countDocuments -> RERANKING_FEATURE_NOT_ENABLED", uncached, COUNT, null),
        error("limit is 0 -> COMMAND_FIELD_VALUE_INVALID", rerank, limit0, null, invalid),
        error("no such collection -> UNKNOWN_COLLECTION_OR_TABLE", MISSING, minimal, null, UNKNOWN),
        args("$vector 1501 -> COMMAND_FIELD_VALUE_INVALID", rerank, overMax, "true", invalid, msg),
        error("hybridLimits 0 -> INVALID_DATABASE_QUERY", rerank, zero, "true", database));
  }

  // Sends findAndRerank with a valid voyageAI override and no reranking header. Expects
  // RERANKING_FEATURE_NOT_ENABLED with collection rerank on or off, cached schema or not.
  // Per issue #2459 (Notes, item 1), an override is not allowed when the reranking feature is off.
  @ParameterizedTest(name = "{0}")
  @MethodSource("overrideTargets")
  void overrideIsRejectedWhenRerankingFeatureIsOff(Fixture fixture) {
    var voyage = Map.of("provider", Model.VOYAGE.provider(), "modelName", Model.VOYAGE.modelName());
    String request = findAndRerank().sort(SORT).option("rerank", voyage).json();
    assertApiError(postToFixture(fixture, request), FEATURE, snippet(FEATURE));
  }

  static Stream<Named<Fixture>> overrideTargets() {
    return Stream.of(
        Named.of("rerank enabled, cached schema -> RERANKING_FEATURE_NOT_ENABLED", OFF_RERANK),
        Named.of("rerank enabled, uncached schema -> RERANKING_FEATURE_NOT_ENABLED", UNCACHED),
        Named.of("rerank disabled -> RERANKING_FEATURE_NOT_ENABLED", OFF_NORERANK));
  }

  // Creates collections with the reranking header while nvidia is disabled, without rerank options
  // or with only {"enabled": true}. Expects findCollections to show rerank on the default nvidia
  // model. The docs do not specify a disabled default provider; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("defaultRerankCollections")
  void collectionWithDefaultRerankIsCreatedWhileNvidiaIsDisabled(Fixture fixture) {
    String path =
        "status.collections.find { it.name == '%s' }.options.rerank".formatted(ensure(fixture));
    String findCollections = "{\"findCollections\": {\"options\": {\"explain\": true}}}";
    postToKeyspace(findCollections, headers(RERANKING, "true"))
        .body("errors", is(nullValue()))
        .body(path, jsonEquals(service(Model.DEFAULT)));
    assertRerankerNotCalled();
  }

  static Stream<Named<Fixture>> defaultRerankCollections() {
    return Stream.of(
        Named.of("no rerank options -> default nvidia model", OFF_RERANK),
        Named.of("rerank enabled without a service -> default nvidia model", ENABLED_ONLY));
  }

  // Sends createCollection with rerank enabled. Expects RERANKING_FEATURE_NOT_ENABLED without the
  // header, but INVALID_CREATE_COLLECTION_OPTIONS for an nvidia service with or without it, as the
  // service is validated first. The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("createCollectionErrorCases")
  void createCollectionWithRerankFailsWhenFeatureOrProviderIsOff(
      String rerank, String header, ErrorCode<?> code) {
    String create = "{\"createCollection\": {\"name\": \"frr_off_x\", \"options\": {\"rerank\": ";
    var response = postToKeyspace(create + rerank + "}}}", headers(RERANKING, header));
    assertApiError(response, code, snippet(code));
  }

  static Stream<Arguments> createCollectionErrorCases() {
    String enabled = "{\"enabled\": true}";
    String voyage = service(Model.VOYAGE);
    String nvidia = service(Model.DEFAULT);
    var invalid = SchemaException.Code.INVALID_CREATE_COLLECTION_OPTIONS;
    return Stream.of(
        args("no service, no header -> RERANKING_FEATURE_NOT_ENABLED", enabled, null, FEATURE),
        args("voyageAI, no header -> RERANKING_FEATURE_NOT_ENABLED", voyage, null, FEATURE),
        args("nvidia, header -> INVALID_CREATE_COLLECTION_OPTIONS", nvidia, "true", invalid),
        args("nvidia, no header -> INVALID_CREATE_COLLECTION_OPTIONS", nvidia, null, invalid));
  }

  // Sends findRerankingProviders for every model status. With the reranking header, expects only
  // the enabled providers, without the disabled default nvidia or jinaAI; without the header,
  // RERANKING_FEATURE_NOT_ENABLED. The docs do not specify this; the test pins current behavior.
  @Test
  void findRerankingProvidersListsOnlyEnabledProviders() {
    String command = "{\"findRerankingProviders\": {\"options\": {\"filterModelStatus\": \"\"}}}";
    var response = postToDatabase(command, headers(RERANKING, "true"));
    response.statusCode(200).body("$", responseIsStatusOnly());
    assertThat(response.extract().<Map<String, Object>>path("status.rerankingProviders"))
        .containsOnlyKeys("cohere", "fakeProvider", "voyageAI");
    assertApiError(postToDatabase(command, headers()), FEATURE, snippet(FEATURE));
  }

  // Sends findAndRerank to a table without the reranking header. Expects UNSUPPORTED_TABLE_COMMAND.
  // The hybrid search docs (page intro) say these features are for collections only, while a
  // design doc planned findAndRerank on tables; main rejects tables before any feature check. The
  // test pins current behavior; whether it is a bug is still under discussion.
  @Test
  void tableGetsUnsupportedTableCommandWhenFeatureIsOff() {
    var code = RequestException.Code.UNSUPPORTED_TABLE_COMMAND;
    assertApiError(postToFixture(OFF_TABLE, REQUEST), code, snippet(code));
  }

  // Calls the findAndRerank MCP tool. Expects a tool error from the first failing step: schema
  // lookup (unknown collection, or a rerank schema that cannot load with reranking off), the MCP
  // flag, then the reranking flag. The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("mcpErrorCases")
  void mcpFindAndRerankWithFeaturesOff(String collection, String mcpHeader, ErrorCode<?> code) {
    var response = callMcp(collection, MCP, mcpHeader);
    response.statusCode(200).body("error", is(nullValue())).body("result.isError", is(true));
    assertThat(response.extract().<Map<String, Object>>path("result.structuredContent"))
        .containsOnlyKeys("errors");
    assertSingleErrorAt(response, "result.structuredContent.errors", code, snippet(code));
    assertRerankerNotCalled();
  }

  static Stream<Arguments> mcpErrorCases() {
    var mcp = SchemaException.Code.MCP_FEATURE_NOT_ENABLED;
    String rerank = OFF_RERANK.name();
    String uncached = UNCACHED.name();
    return Stream.of(
        args("MCP off, rerank collection -> MCP_FEATURE_NOT_ENABLED", rerank, null, mcp),
        args("MCP off, no such collection -> UNKNOWN_COLLECTION_OR_TABLE", MISSING, null, UNKNOWN),
        args("MCP off, table -> MCP_FEATURE_NOT_ENABLED", OFF_TABLE.name(), null, mcp),
        args("MCP on, cached schema -> RERANKING_FEATURE_NOT_ENABLED", rerank, "true", FEATURE),
        args("MCP on, uncached schema -> RERANKING_FEATURE_NOT_ENABLED", uncached, "true", FEATURE),
        args("MCP off, uncached schema -> RERANKING_FEATURE_NOT_ENABLED", uncached, null, FEATURE));
  }

  // Calls the findAndRerank MCP tool with the MCP and the reranking header, and no limit. Expects
  // one reranker request with both passages, then only f2 as the configured default limit is 1.
  // The docs do not specify this; the test pins current behavior.
  @Test
  void mcpWithBothFeatureHeadersReranks() {
    var response = callMcp(OFF_RERANK.name(), MCP, "true", RERANKING, "true");
    response.statusCode(200).body("error", is(nullValue())).body("result.isError", is(false));
    assertThat(response.extract().jsonPath().getList("result.structuredContent.documents._id"))
        .containsExactly("f2");
    assertRerankedOnce("f2", "f1");
  }

  // Sends hybridLimits that only this class allows and reads each CQL LIMIT from the trace. Expects
  // the vector read capped at 1000 and, on HCD, the BM25 read capped at 1000, or 100 (its default)
  // for 0; DSE has no BM25 read. $lexical 1550 is accepted as each limit has its own bounds, and
  // only the $vector maximum is 1500. The docs do not specify this; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource("hybridLimitsTraceCases")
  void configuredHybridLimitsBoundsAndReadLimits(
      String options, int vectorLimit, int bm25Limit, List<String> ids) {
    String request = findAndRerank().hybrid(QUERY).options(options).json();
    var response = postToFixture(OFF_RERANK, request, headers(RERANKING, "true", TRACING, "true"));
    assertIds(response, ids.toArray());
    String body = response.extract().asString();
    Set<Integer> bm25Limits = lexicalAvailable() ? Set.of(bm25Limit) : Set.of();
    assertThat(readLimits(body, "ANN OF ?")).as("vector read LIMIT").containsExactly(vectorLimit);
    assertThat(readLimits(body, "BM25 OF ?")).as("BM25 read LIMIT").isEqualTo(bm25Limits);
    assertRerankedOnce(ids.toArray());
  }

  static Stream<Arguments> hybridLimitsTraceCases() {
    // limit 2 returns both reranked documents (the configured default is 1) and leaves reads alone.
    String all1500 = "{\"hybridLimits\": 1500, \"limit\": 2}";
    String lexical0 = "{\"hybridLimits\": {\"$vector\": 1, \"$lexical\": 0}}";
    String lexical1550 = "{\"hybridLimits\": {\"$vector\": 1, \"$lexical\": 1550}}";
    var both = List.of("f2", "f1");
    var f1 = List.of("f1");
    return Stream.of(
        args("hybridLimits 1500 -> both reads use LIMIT 1000", all1500, 1000, 1000, both),
        args("$vector 1, $lexical 0 -> BM25 read uses LIMIT 100", lexical0, 1, 100, f1),
        args("$vector 1, $lexical 1550 -> BM25 read uses LIMIT 1000", lexical1550, 1, 1000, f1));
  }

  private static String snippet(ErrorCode<?> code) {
    return switch (code.name()) {
      case "RERANKING_FEATURE_NOT_ENABLED" -> "Reranking feature is not enabled for this database.";
      case "MCP_FEATURE_NOT_ENABLED" -> "MCP feature is not enabled for this database.";
      case "UNKNOWN_COLLECTION_OR_TABLE" -> "does not exist in the Keyspace";
      case "COMMAND_FIELD_VALUE_INVALID" -> "limit should be greater than `0`";
      case "INVALID_CREATE_COLLECTION_OPTIONS" -> "Reranking provider 'nvidia' is disabled";
      case "UNSUPPORTED_TABLE_COMMAND" -> "The command is not supported by tables in the API.";
      case "INVALID_DATABASE_QUERY" -> "LIMIT must be strictly positive";
      default -> throw new IllegalArgumentException("No snippet for " + code.name());
    };
  }

  /** A collection created with the reranking header, without documents. */
  private static Fixture created(String name, String options) {
    return new Fixture(name, false, options, options, OFF_RERANK.createHeaders(), List.of());
  }

  private static String service(Model model) {
    return "{\"enabled\": true, \"service\": {\"provider\": \"%s\", \"modelName\": \"%s\"}}"
        .formatted(model.provider(), model.modelName());
  }

  /** One reranker request on the default model, for the sort text and these documents. */
  private static void assertRerankedOnce(Object... ids) {
    String[] passages = Arrays.stream(ids).map(PASSAGES::get).toArray(String[]::new);
    assertRerankerCalls(calls(1).query(QUERY).passages(passages));
  }

  /** A case that expects RERANKING_FEATURE_NOT_ENABLED; a null header value sends no header. */
  private static Arguments off(String desc, String collection, String request, String header) {
    return error(desc, collection, request, header, FEATURE);
  }

  private static Arguments error(
      String desc, String collection, String request, String header, ErrorCode<?> code) {
    return args(desc, collection, request, header, code, snippet(code));
  }

  /** Arguments whose first value carries the case description shown in the test report. */
  private static Arguments args(String description, Object first, Object... rest) {
    Stream<Object> named = Stream.of(Named.of(description, first));
    return Arguments.of(Stream.concat(named, Arrays.stream(rest)).toArray());
  }

  /** Calls the findAndRerank MCP tool with the $vectorize sort, as one JSON-RPC request. */
  private ValidatableResponse callMcp(String collection, Object... headerPairs) {
    var headers = headers(headerPairs);
    headers.put("Accept", "application/json, text/event-stream");
    String call =
        """
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "findAndRerank",
          "arguments": {"keyspace": "%s", "collection": "%s", "sort": %s}}}""";
    return post("/v1/mcp", call.formatted(keyspace(), collection, SORT), headers);
  }

  /** The distinct numbers that follow "{clause} LIMIT " in the response text. */
  private static Set<Integer> readLimits(String body, String clause) {
    var matcher = Pattern.compile(Pattern.quote(clause) + " LIMIT (\\d+)").matcher(body);
    return matcher.results().map(m -> Integer.parseInt(m.group(1))).collect(Collectors.toSet());
  }
}
