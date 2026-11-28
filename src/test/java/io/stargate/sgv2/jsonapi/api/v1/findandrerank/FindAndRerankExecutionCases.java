package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.combine;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.mode;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertScores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSuccessEnvelope;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.rrf;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.scores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.BYO;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.EXAMPLE_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS_TERM;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.IDS_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.MAIN;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NOLEX;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NORERANK;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.headers;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.parse;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerNotCalled;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.calls;
import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Key;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.ExpectedScores;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests about how the command runs: what each read finds, how candidates are merged
 * and dropped, the request sent to the reranker, batching, the final order and ties.
 *
 * <p>For reviewers: expected orders, ranks and scores follow from the fixture data ({@link
 * FindAndRerankFixtures} has the vector and BM25 orders, {@link FakeRerankerScores} the rerank
 * scores). With {@code includeScores} every score of every returned document is asserted. No test
 * asserts the vector ranks of frr_main m06 to m12, which share one vector. BM25 tests need HCD.
 */
public interface FindAndRerankExecutionCases extends FindAndRerankTestContext {

  String EXECUTION_FACTORIES =
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankExecutionCases#";

  String TREE = "A tree in the woods"; // has no test vector; its embedding is that of m06 to m12
  String SORT_TREE = "{\"$hybrid\": {\"$vectorize\": \"" + TREE + "\"}}";
  String SORT_CHATGPT = "{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\"}}";
  String BYO_QUERY = "house hill grassy";
  float NOT_IN_VECTOR_READ = Integer.MIN_VALUE; // $vector of a document the vector read missed

  // Sends a $hybrid string filtered to m02-m05 with includeScores and limit 3. Expects both reads
  // filtered, the four passages reranked and the best three returned with both ranks. Per the
  // hybrid search docs (The hybrid search process); the unspecified rank and RRF values are pinned.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void hybridStringRunsBothFilteredReadsThenReranksAndTruncates() {
    var request = findAndRerank().filter("{\"grp\": \"distinct\", \"is_checked_out\": false}");
    request.hybrid(TREE).options("{\"includeScores\": true, \"limit\": 3}");
    var response = postToFixture(MAIN, request.json());
    assertIds(response, "m05", "m03", "m02"); // m01, the best BM25 match, is filtered out
    assertRerankerCalls(calls(1).query(TREE).passages(texts("m02", "m03", "m04", "m05")));
    assertScores(
        response,
        scores(3.25f, 0.942786f, 2, 4, rrf(2, 4)),
        scores(2.125f, 0.86899745f, 4, 3, rrf(4, 3)),
        scores(1.5f, 0.9312073f, 3, 1, rrf(3, 1)));
  }

  // Reranks the four "order" documents of frr_byo on title. Expects the rerank order, unlike the
  // vector, BM25, _id and RRF orders (o3 has a higher RRF than o4). The findAndRerank docs (Include
  // the scores) say vector and reranker scores are compared together; main sorts by the rerank
  // score alone. The test pins current behavior; whether it is a bug is still under discussion.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void rerankScoreAloneOrdersResultsAgainstVectorBm25AndIdOrders() {
    var response = postToFixture(BYO, byoRequest("title").filter("{\"grp\": \"order\"}").json());
    List<Object> rerankOrder = List.of("o4", "o3", "o2", "o1");
    assertIds(response, rerankOrder.toArray());
    assertRerankerCalls(calls(1).query(BYO_QUERY).passages(fieldOf(BYO, "title", rerankOrder)));
    // By these ranks the vector order is o3 o1 o4 o2 and the BM25 order is o2 o4 o1 o3.
    assertScores(
        response,
        scores(3.0f, 0.8039639f, 3, 2, rrf(3, 2)),
        scores(2.0f, 0.9996167f, 1, 4, rrf(1, 4)),
        scores(0.75f, 0.5678263f, 4, 1, rrf(4, 1)),
        scores(-0.25f, 0.9535399f, 2, 3, rrf(2, 3)));
  }

  // Sends includeScores requests whose candidates lack $vector or $lexical or have a null $lexical.
  // Expects each reranked when one read finds it and it has a passage; one without the passage
  // field is dropped. The findAndRerank and hybrid search docs say such documents are excluded;
  // main keeps them. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "incompleteDocumentCases")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void documentsMissingVectorOrLexicalAreRerankedWhenOneReadFindsThem(
      Fixture fixture,
      String command,
      String passageField,
      String query,
      List<Object> ids,
      List<ExpectedScores> expectedScores) {
    var response = postToFixture(fixture, command);
    assertIds(response, ids.toArray());
    assertRerankerCalls(calls(1).query(query).passages(fieldOf(fixture, passageField, ids)));
    assertScores(response, expectedScores.toArray(ExpectedScores[]::new));
  }

  static Stream<Arguments> incompleteDocumentCases() {
    String m08Only = "{\"flag\": true}";
    return Stream.of(
        Arguments.of(
            Named.of("frr_byo on $lexical -> b5 without $vector reranked, b6 and b7 dropped", BYO),
            byoRequest("$lexical").json(),
            "$lexical",
            BYO_QUERY,
            List.of("b5", "o4", "o3", "b9", "o2", "b8", "o1"),
            List.of(
                scores(4.25f, NOT_IN_VECTOR_READ, null, 1, rrf(1)),
                scores(3.5f, 0.8039639f, 5, 3, rrf(5, 3)),
                scores(2.25f, 0.9996167f, 1, 5, rrf(1, 5)),
                scores(1.75f, 0.98955715f, 2, 6, rrf(2, 6)),
                scores(1.0f, 0.5678263f, 8, 2, rrf(8, 2)),
                scores(0.125f, 0.6669121f, 7, null, rrf(7)),
                scores(-0.5f, 0.9535399f, 3, 4, rrf(3, 4)))),
        Arguments.of(
            Named.of("frr_byo on title -> b7 with a null $lexical is found by vector only", BYO),
            byoRequest("title").json(),
            "title",
            BYO_QUERY,
            List.of("o4", "o3", "b7", "o2", "b6", "o1"),
            List.of(
                scores(3.0f, 0.8039639f, 5, 3, rrf(5, 3)),
                scores(2.0f, 0.9996167f, 1, 5, rrf(1, 5)),
                scores(1.5f, 0.7557548f, 6, null, rrf(6)),
                scores(0.75f, 0.5678263f, 8, 2, rrf(8, 2)),
                scores(0.25f, 0.920735f, 4, null, rrf(4)),
                scores(-0.25f, 0.9535399f, 3, 4, rrf(3, 4)))),
        // "pumpkin" is in the $vectorize text of m08, but no $lexical is derived from it.
        Arguments.of(
            Named.of("frr_main $hybrid string -> m08 without $lexical found by vector only", MAIN),
            findAndRerank().filter(m08Only).hybrid("pumpkin").option("includeScores", true).json(),
            "$vectorize",
            "pumpkin",
            List.of("m08"),
            List.of(scores(3.75f, 1.0f, 1, null, rrf(1)))));
  }

  // Reranks frr_main's five "distinct" documents on title; the sort vector is $vectorize text, or a
  // $vector array or $binary without the embedding key. Expects the same ranks and scores. Per the
  // findAndRerank docs (Parameters, sort); that no key is needed is unspecified and pinned.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "sortVectorCases")
  default void vectorReadRanksBySortVectorWhetherEmbeddedOrGiven(
      String sort, Map<String, Object> requestHeaders) {
    var request = findAndRerank().filter("{\"grp\": \"distinct\"}").sort(sort);
    request.options("{\"rerankOn\": \"title\", \"rerankQuery\": \"q\", \"includeScores\": true}");
    var response = postToFixture(MAIN, request.json(), requestHeaders);
    List<Object> ids = List.of("m01", "m03", "m05", "m02", "m04");
    assertIds(response, ids.toArray());
    assertRerankerCalls(calls(1).query("q").passages(fieldOf(MAIN, "title", ids)));
    assertScores(
        response,
        scores(2.25f, 0.7665279f, 5, null, rrf(5)),
        scores(1.125f, 0.8224976f, 4, null, rrf(4)),
        scores(0.875f, 0.93070626f, 3, null, rrf(3)),
        scores(-0.5f, 0.9787127f, 1, null, rrf(1)),
        scores(-1.625f, 0.9768599f, 2, null, rrf(2)));
  }

  static Stream<Arguments> sortVectorCases() {
    // The test embedding provider's vector for "ChatGPT upgraded", and the Base64 of the same five
    // floats as big-endian float32. The test embedding provider fails every call without the key.
    String array = "[0.1, 0.16, 0.31, 0.22, 0.15]";
    String binary = "{\"$binary\": \"PczMzT4j1wo+nrhSPmFHrj4ZmZo=\"}";
    var noKey = headers(EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, null);
    return Stream.of(
        Arguments.of(Named.of("$vectorize text -> its embedding", SORT_CHATGPT), headers()),
        Arguments.of(Named.of("$vector array, no embedding key", vectorSort(array)), noKey),
        Arguments.of(Named.of("$binary vector, no embedding key", vectorSort(binary)), noKey));
  }

  // Sends $vectorize requests without the embedding key that fail a check made while the command is
  // built. Expects that check's error, not the test provider's missing-key error, and no reranker
  // call. The docs do not specify this order; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "buildErrorCases")
  default void buildErrorsAreReportedBeforeAnyEmbeddingCall(
      Fixture fixture, String command, ErrorCode<?> code, String messageSnippet) {
    var noKey = headers(EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, null);
    assertApiError(postToFixture(fixture, command, noKey), code, messageSnippet);
  }

  static Stream<Arguments> buildErrorCases() {
    var badModel = Map.of("provider", "nvidia", "modelName", "nvidia/no-such-model");
    String lexicalSort =
        "{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\", \"$lexical\": \"x\"}}";
    return Stream.of(
        Arguments.of(
            Named.of("hybridLimits 101 -> COMMAND_FIELD_VALUE_INVALID", MAIN),
            findAndRerank().sort(SORT_CHATGPT).option("hybridLimits", 101).json(),
            RequestException.Code.COMMAND_FIELD_VALUE_INVALID,
            "must be between 1 and 100"),
        Arguments.of(
            Named.of("override with an unknown model -> INVALID_RERANK_OVERRIDE", MAIN),
            findAndRerank().sort(SORT_CHATGPT).option("rerank", badModel).json(),
            RequestException.Code.INVALID_RERANK_OVERRIDE,
            "'nvidia/no-such-model' is not supported"),
        Arguments.of(
            Named.of("rerank disabled, no override -> UNSUPPORTED_RERANKING_COMMAND", NORERANK),
            findAndRerank().sort(SORT_CHATGPT).json(),
            RequestException.Code.UNSUPPORTED_RERANKING_COMMAND,
            "is not enabled for the collection"),
        Arguments.of(
            Named.of("$lexical without lexical -> LEXICAL_NOT_ENABLED_FOR_COLLECTION", NOLEX),
            findAndRerank().sort(lexicalSort).json(),
            SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
            "frr_nolex"));
  }

  // Sends requests that leave nothing to rerank, one with a blank reranking-api-key. Expects an
  // empty page, no reranker call and, when traced, a trace that the call was skipped (fully traced,
  // with the query, limit and no passages). The findAndRerank docs (Parameters, rerankOn) exclude
  // documents without that field; the rest is unspecified and pinned.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "nothingToRerankCases")
  default void nothingLeftToRerankSkipsTheReranker(
      Fixture fixture,
      String command,
      Map<String, Object> requestHeaders,
      List<String> trace,
      Map<String, Object> skippedData) {
    var response = assertIds(postToFixture(fixture, command, requestHeaders));
    assertRerankerNotCalled();
    if (trace == null) {
      assertSuccessEnvelope(response); // not traced: no status, so no status.trace
    } else {
      assertSuccessEnvelope(response, "trace");
      assertThat(response.extract().jsonPath().getString("status.trace"))
          .containsSubsequence(trace)
          .doesNotContain("passages using"); // no "Reranking N passages using ..." message
    }
    if (skippedData != null) {
      String path =
          "status.trace.TraceSession.events.TraceEvent"
              + ".find { it.message.startsWith('Reranking call skipped') }.data.RecordableMap";
      var data = new HashMap<String, Object>(response.extract().jsonPath().getMap(path));
      // The JSON writes the limit as 10.0, so it is compared as a number.
      data.computeIfPresent("limit", (key, limit) -> ((Number) limit).doubleValue());
      assertThat(data).as("data of the skipped message").isEqualTo(skippedData);
    }
  }

  static Stream<Arguments> nothingToRerankCases() {
    String grpNone = "{\"grp\": \"none\"}";
    String noMatch = findAndRerank().filter(grpNone).sort(SORT_CHATGPT).json();
    var second = Map.of("provider", "nvidia", "modelName", Model.SECOND.modelName());
    String overridden =
        findAndRerank().filter(grpNone).sort(SORT_CHATGPT).option("rerank", second).json();
    String opts = "{\"rerankOn\": \"content\", \"rerankQuery\": \"q\"}";
    String noField = findAndRerank().sort(vectorSort(EXAMPLE_VECTOR)).options(opts).json();
    var blankKey = headers(RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME, "   ");
    var traced = headers(ApiFeature.REQUEST_TRACING.httpHeaderName(), "true");
    var fullyTraced = headers(ApiFeature.REQUEST_TRACING_FULL.httpHeaderName(), "true");
    // The default limit is 10; the request has no rerankQuery, so the query is the sort text.
    var query = Map.of("query", "ChatGPT upgraded", "source", "VECTORIZE");
    Map<String, Object> skippedData =
        Map.of("query", Map.of("RerankingQuery", query), "limit", 10.0, "passages", List.of());
    return Stream.of(
        Arguments.of(Named.of("filter matches nothing", MAIN), noMatch, headers(), null, null),
        Arguments.of(
            Named.of("no candidate has the rerankOn field", BYO), noField, headers(), null, null),
        Arguments.of(
            Named.of("no match, blank reranking-api-key", MAIN), noMatch, blankKey, null, null),
        Arguments.of(
            Named.of("filter matches nothing, request tracing on", MAIN),
            noMatch,
            traced,
            skippedRerankTrace(0, Model.DEFAULT),
            null),
        // frr_byo has eight documents with a vector; the vector read finds all and drops all.
        Arguments.of(
            Named.of("no candidate has the rerankOn field, request tracing on", BYO),
            noField,
            traced,
            skippedRerankTrace(8, Model.DEFAULT),
            null),
        Arguments.of(
            Named.of("filter matches nothing, override, full request tracing on", MAIN),
            overridden,
            fullyTraced,
            skippedRerankTrace(0, Model.SECOND),
            skippedData));
  }

  // Reranks "A tree in the woods" on frr_main with 10, 11 or 12 passages, and on its 7 "plain"
  // documents with a batch size 3 model and limit 1. Expects one request per batch, each a slice
  // of one list in the order of an internal map keyed by _id, all passages sent despite the limit,
  // each score given to its document. The docs do not specify batching; the test pins it.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "batchCases")
  default void passagesAreSplitIntoBatchesInInternalMapOrder(
      Model model, String filter, String options, List<Object> ids, List<List<String>> idBatches) {
    String command = findAndRerank().filter(filter).sort(SORT_TREE).options(options).json();
    assertIds(postToFixture(MAIN, command), ids.toArray());
    var batches = idBatches.stream().map(b -> List.of(fieldOf(MAIN, "$vectorize", b))).toList();
    var passages = batches.stream().flatMap(List::stream).toArray(String[]::new);
    assertRerankerCalls(calls(batches.size()).on(model).query(TREE).passages(passages));
    assertThat(FakeRerankerClient.requests().stream().map(RecordedRerankRequest::passages).toList())
        .as("passages of each request, in order within the request")
        .containsExactlyInAnyOrderElementsOf(batches);
  }

  static Stream<Arguments> batchCases() {
    var noM01OrM03 = List.of("m04", "m06", "m05", "m08", "m07", "m09", "m11", "m10", "m02", "m12");
    String toSecondModel =
        "{\"limit\": 1, \"rerank\": {\"provider\": \"nvidia\", \"modelName\": \"%s\"}}"
            .formatted(Model.SECOND.modelName());
    return Stream.of(
        Arguments.of(
            Named.of("hybridLimits 11 -> 10 passages in one request", Model.DEFAULT),
            "{}",
            "{\"hybridLimits\": 11}",
            List.of("m08", "m05", "m11", "m06", "m02", "m12", "m04", "m09", "m07", "m10"),
            List.of(noM01OrM03)),
        Arguments.of(
            Named.of("hybridLimits 12 -> 11 passages in requests of 10 and 1", Model.DEFAULT),
            "{}",
            "{\"hybridLimits\": 12}",
            List.of("m08", "m05", "m11", "m06", "m02", "m12", "m04", "m09", "m01", "m07"),
            List.of(noM01OrM03, List.of("m01"))),
        Arguments.of(
            Named.of("default read size -> 12 passages in requests of 10 and 2", Model.DEFAULT),
            "{}",
            "{}",
            List.of("m08", "m05", "m11", "m03", "m06", "m02", "m12", "m04", "m09", "m01"),
            List.of(
                List.of("m04", "m03", "m06", "m05", "m08", "m07", "m09", "m11", "m10", "m02"),
                List.of("m12", "m01"))),
        Arguments.of(
            Named.of("batch size 3, limit 1 -> 7 passages in requests of 3, 3 and 1", Model.SECOND),
            "{\"grp\": \"plain\"}",
            toSecondModel,
            List.of("m08"),
            List.of(List.of("m06", "m08", "m07"), List.of("m09", "m11", "m10"), List.of("m12"))));
  }

  // Sends $vectorize "A tree in the woods" to frr_main (12 passages, two requests); the fake holds
  // each request 1 s and records whether the other request arrived during that time. Expects the
  // usual result and a request that saw the other arrive, so the second batch went out before the
  // first was answered. The docs do not specify this; the test pins current behavior.
  @Test
  default void passageBatchesAreSentConcurrently() {
    String query = combine(mode(Key.DELAY_MS, 1000), mode(Key.REQUIRE_CONCURRENT));
    FakeRerankerClient.stubMode(query);
    String command = findAndRerank().sort(SORT_TREE).option("rerankQuery", query).json();
    Object[] ids = {"m08", "m05", "m11", "m03", "m06", "m02", "m12", "m04", "m09", "m01"};
    assertIds(postToFixture(MAIN, command), ids);
    var passages =
        texts("m01", "m02", "m03", "m04", "m05", "m06", "m07", "m08", "m09", "m10", "m11", "m12");
    assertRerankerCalls(calls(2).query(query).passages(passages).batchSizes(10, 2));
    // Only the request that arrived first is sure to see the other arrive during its 1 s hold.
    // Sent one after another, the second would arrive after that hold, and both would be false.
    assertThat(FakeRerankerClient.requests())
        .extracting(RecordedRerankRequest::concurrent)
        .as("for each request, whether the other one was in flight")
        .doesNotContainNull()
        .contains(true);
  }

  // Reranks the frr_ids documents with the _ids string "1" and number 1 on title. Expects both to
  // be reranked and returned: candidates are merged by exact _id, not by its text. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @Test
  default void stringAndNumberIdsWithTheSameTextAreSeparateCandidates() {
    assertIds(postToFixture(IDS, idsRequest("grp", "dedup").json()), 1, "1");
    assertRerankerCalls(calls(1).query(IDS_TERM).passages("Number one", "String one"));
  }

  // Reranks frr_ids groups whose titles get equal rerank scores, without and with includeScores.
  // Expects all passages sent and ties broken by higher RRF, then by _id: JSON type first (boolean,
  // number, string), then the plain _id text, so 10 before 9 and "q" before "q!"; similarity and
  // includeScores do not matter. The docs do not specify ties; the test pins current behavior.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "tieCases")
  default void equalRerankScoresAreOrderedByRrfThenId(
      String group, int readSize, List<Object> ids, List<ExpectedScores> expectedScores) {
    // readSize 0: vector read only; otherwise both reads, each limited to readSize documents.
    assumeTrue(readSize == 0 || lexicalAvailable(), "needs a BM25 read (HCD)");
    for (boolean includeScores : new boolean[] {false, true}) {
      FakeRerankerClient.reset();
      var request = idsRequest("pairs", group).option("includeScores", includeScores);
      if (readSize > 0) {
        request
            .sort(hybridSort(IDS_VECTOR, IDS_TERM))
            .option("hybridLimits", Map.of("$vector", readSize, "$lexical", readSize));
      }
      var response = postToFixture(IDS, request.json());
      assertIds(response, ids.toArray());
      assertRerankerCalls(calls(1).query(IDS_TERM).passages(fieldOf(IDS, "title", ids)));
      if (includeScores) {
        assertScores(response, expectedScores.toArray(ExpectedScores[]::new));
      }
    }
  }

  static Stream<Arguments> tieCases() {
    var v = scores(1.0f, 1.0f, 1, null, rrf(1)); // found by the vector read only, at rank 1
    var b = scores(1.0f, NOT_IN_VECTOR_READ, null, 1, rrf(1)); // by the BM25 read only, at rank 1
    return Stream.of(
        Arguments.of(
            Named.of("p-vec, vector read only -> rank 1 first", "p-vec"),
            0,
            List.of("vb", "va"),
            List.of(v, scores(1.0f, 0.8535534f, 2, null, rrf(2)))),
        Arguments.of(
            Named.of("p-rrf -> X in both reads before Y in one", "p-rrf"),
            2,
            List.of("Z", "X", "Y"),
            List.of(
                scores(2.5f, NOT_IN_VECTOR_READ, null, 1, rrf(1)),
                scores(1.0f, 0.8535534f, 2, 2, rrf(2, 2)),
                v)),
        Arguments.of(
            Named.of("p-str -> \"a\" before \"b\"", "p-str"), 1, List.of("a", "b"), List.of(v, b)),
        // Compared as quoted JSON text, "q!" would come first, because '!' sorts before '"'.
        Arguments.of(
            Named.of("p-prefix -> \"q\" before \"q!\"", "p-prefix"),
            1,
            List.of("q", "q!"),
            List.of(v, b)),
        Arguments.of(Named.of("p-num -> 10 before 9", "p-num"), 1, List.of(10, 9), List.of(b, v)),
        Arguments.of(
            Named.of("p-type1 -> true before 5", "p-type1"), 1, List.of(true, 5), List.of(v, b)),
        Arguments.of(
            Named.of("p-type2 -> 5 before \"c\"", "p-type2"), 1, List.of(5, "c"), List.of(b, v)));
  }

  // Reads one frr_ids document whose _id is an object (with or without a passage) or null.
  // Expects the whole request to fail with UNEXPECTED_SERVER_ERROR and no reranker call. The docs
  // do not mention such _id values, and main answers with a server error instead of a documented
  // one. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "unusableIdCases")
  default void objectOrNullIdCandidateFailsTheWholeRequest(
      String group, String rerankOn, String messageSnippet) {
    assertApiError(
        postToFixture(IDS, idsRequest("grp", group).option("rerankOn", rerankOn).json()),
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "Error Class: IllegalArgumentException",
        messageSnippet);
  }

  static Stream<Arguments> unusableIdCases() {
    String objectId = "id must be a value node";
    return Stream.of(
        Arguments.of(Named.of("object _id, with a passage", "objid"), "title", objectId),
        Arguments.of(Named.of("object _id, without a passage", "objid"), "no_such_field", objectId),
        Arguments.of(Named.of("null _id", "nullid"), "title", "Document id cannot be a NullNode"));
  }

  // Sends $vectorize text to frr_main, with and without a reranking-api-key. Expects one POST to
  // the model URL as configured, a body of only model, query.text, passages[].text and truncate
  // NONE, JSON content type, tenant-id SINGLE-TENANT, and Authorization "Bearer " plus that key or
  // else the Data API token. The docs do not specify the reranker request; the test pins it.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "rerankerAuthCases")
  default void rerankerRequestHasFixedBodyAndAuthorizationHeader(
      Map<String, Object> requestHeaders, String rerankingApiKey) {
    String command = findAndRerank().filter("{\"grp\": \"distinct\"}").sort(SORT_CHATGPT).json();
    assertIds(postToFixture(MAIN, command, requestHeaders), "m05", "m03", "m02", "m04", "m01");
    var expected =
        calls(1).query("ChatGPT upgraded").passages(texts("m01", "m02", "m03", "m04", "m05"));
    assertRerankerCalls(
        rerankingApiKey == null ? expected : expected.rerankingApiKey(rerankingApiKey));
    RecordedRerankRequest request = FakeRerankerClient.requests().getFirst();
    assertThat(request.header("Content-Type")).isEqualTo("application/json");
    var body = parse(request.body());
    var bodyFields = List.of("model", "query", "passages", "truncate");
    assertThat(body.fieldNames()).toIterable().containsExactlyInAnyOrderElementsOf(bodyFields);
    assertThat(body.get("query").fieldNames()).toIterable().containsExactly("text");
    body.get("passages")
        .forEach(p -> assertThat(p.fieldNames()).toIterable().containsExactly("text"));
  }

  static Stream<Arguments> rerankerAuthCases() {
    var withKey = headers(RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME, "my-key");
    return Stream.of(
        Arguments.of(Named.of("no reranking-api-key -> Data API token", headers()), null),
        Arguments.of(Named.of("reranking-api-key my-key -> my-key", withKey), "my-key"));
  }

  // Sends a $vector plus $lexical sort that reads 1 document by vector and up to 10 by BM25,
  // reranking on $lexical, so other candidates come from the BM25 read. Expects BM25 to find just
  // the documents whose $lexical contains every query word. Per the hybrid search docs (The hybrid
  // search process), which also say "These boots were made for hiking, ..." matches the words.
  @ParameterizedTest(name = "{0}")
  @MethodSource(EXECUTION_FACTORIES + "bm25MatchCases")
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void bm25ReadFindsOnlyDocumentsWithEveryQueryWord(
      Fixture fixture, String filter, String vector, String words, List<Object> ids) {
    var request = findAndRerank().filter(filter).sort(hybridSort(vector, words));
    request.option("hybridLimits", Map.of("$vector", 1, "$lexical", 10));
    request.option("rerankOn", "$lexical").option("rerankQuery", words);
    assertIds(postToFixture(fixture, request.json()), ids.toArray());
    assertRerankerCalls(calls(1).query(words).passages(fieldOf(fixture, "$lexical", ids)));
  }

  static Stream<Arguments> bm25MatchCases() {
    String nearMiss = "[1.0, 1.0, 0.0]"; // the vector of nm-miss
    return Stream.of(
        nomatchCase("kiwi -> BM25 finds nm-hit only", IDS_VECTOR, "kiwi", "nm-hit"),
        nomatchCase(
            "cherry -> BM25 finds nm-hit and nm-miss", IDS_VECTOR, "cherry", "nm-hit", "nm-miss"),
        nomatchCase(
            "kiwi durian, vector finds nm-miss -> BM25 finds neither",
            nearMiss,
            "kiwi durian",
            "nm-miss"),
        Arguments.of(
            Named.of("waterproof hiking boots, vector finds o3 -> BM25 finds b8", BYO),
            "{}",
            EXAMPLE_VECTOR,
            "waterproof hiking boots",
            List.of("o3", "b8")));
  }

  /** A bm25MatchCases case on the frr_ids group grp "nomatch"; ids is the expected result. */
  private static Arguments nomatchCase(String name, String vector, String words, Object... ids) {
    return Arguments.of(Named.of(name, IDS), "{\"grp\": \"nomatch\"}", vector, words, List.of(ids));
  }

  /** The text of {@code field} in the fixture document of each _id; $hybrid gives $vectorize. */
  private static String[] fieldOf(Fixture fixture, String field, List<?> ids) {
    var docs = fixture.documents(!field.equals("$vectorize")).stream().map(d -> parse(d)).toList();
    return ids.stream()
        .map(id -> parse(id instanceof String s ? "\"" + s + "\"" : String.valueOf(id)))
        .map(id -> docs.stream().filter(d -> id.equals(d.get("_id"))).findFirst().orElseThrow())
        .map(doc -> doc.get(field).asText())
        .toArray(String[]::new);
  }

  /** The {@code $vectorize} texts of these frr_main documents, in this order. */
  private static String[] texts(String... mainIds) {
    return fieldOf(MAIN, "$vectorize", List.of(mainIds));
  }

  /** The trace messages, in order, when n documents are read, all dropped, and none reranked. */
  private static List<String> skippedRerankTrace(int n, Model model) {
    String using = " using NvidiaRerankingProvider with model " + model.modelName();
    return List.of(
        "reads=2, total documents=" + n + ", dropped documents=" + n + ", deduplicated documents=0",
        "Reranking call skipped because 0 passages to rerank" + using,
        "Processing reranking response with 0 scores" + using);
  }

  private static String vectorSort(String vectorJson) {
    return "{\"$hybrid\": {\"$vector\": " + vectorJson + "}}";
  }

  private static String hybridSort(String vectorJson, String lexical) {
    return "{\"$hybrid\": {\"$vector\": " + vectorJson + ", \"$lexical\": \"" + lexical + "\"}}";
  }

  /** All of frr_byo: both reads with house hill grassy, includeScores, this rerankOn. */
  private static FindAndRerankRequests.Builder byoRequest(String rerankOn) {
    return findAndRerank()
        .sort(hybridSort(EXAMPLE_VECTOR, BYO_QUERY))
        .options("{\"rerankQuery\": \"house hill grassy\", \"includeScores\": true}")
        .option("rerankOn", rerankOn);
  }

  /** One frr_ids group, vector-only sort, reranked on title with the BM25 term as query. */
  private static FindAndRerankRequests.Builder idsRequest(String groupField, String group) {
    return findAndRerank()
        .filter("{\"%s\": \"%s\"}".formatted(groupField, group))
        .sort(vectorSort(IDS_VECTOR))
        .options("{\"rerankOn\": \"title\", \"rerankQuery\": \"%s\"}".formatted(IDS_TERM));
  }
}
