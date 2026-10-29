package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.AbstractKeyspaceIntegrationTestBase.TEST_PROP_LEXICAL_DISABLED;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertApiError;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertNoDollarFields;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertScores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSortVector;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSuccessEnvelope;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.rrf;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.scores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.BYO;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.EXAMPLE_VECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.MAIN;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NOLEX;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NOVECTOR;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.passages;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.headers;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.defaultModelCalls;
import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Key;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.Builder;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.ProjectionException;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests about the response format: {@code data.documents}, the scores in {@code
 * status.documentResponses}, {@code status.sortVector}, {@code nextPageState}, and the shape of
 * error responses. The tests run as methods of {@code FindAndRerankFakeRerankerIntegrationTest}.
 *
 * <p>Most tests use the five documents of {@code grp: "distinct"} in {@link
 * FindAndRerankFixtures#MAIN}, whose vector ranks are fixed; the expected vector scores and ranks
 * come from the orders listed there. Vector scores are compared within 1e-6, because the last digit
 * differs between HCD and DSE.
 *
 * <p>For review: the error test checks the envelope of each error, not whether the code is the
 * right one; the codes are pinned by the tests of each feature.
 */
public interface FindAndRerankResponseCases extends FindAndRerankTestContext {

  /** Filter on the five documents of {@code frr_main} whose vector ranks are fixed. */
  String DISTINCT_FILTER = "{\"grp\": \"distinct\"}";

  /** Vector-only sort; its vector order on the distinct documents is m02, m04, m05, m03, m01. */
  String CHATGPT_SORT = "{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\"}}";

  /** Which of includeScores and includeSortVector a request sets; null leaves the option out. */
  record ScoreOptions(Boolean includeScores, Boolean includeSortVector) {}

  // Sends a $vectorize sort with includeScores and includeSortVector left out, false or true.
  // Expects status to hold exactly the requested parts (five scores per document, the query
  // embedding), or no status, and a null nextPageState. The docs (Result section) agree but show
  // only $rerank and $vector in scores; main returns five keys and no query, which an early design
  // put in status. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankResponseCases#scoreOptions")
  default void statusHoldsOnlyTheRequestedScoresAndSortVector(ScoreOptions options) {
    Builder request = findAndRerank().filter(DISTINCT_FILTER).sort(CHATGPT_SORT);
    if (options.includeScores() != null) {
      request.option("includeScores", options.includeScores());
    }
    if (options.includeSortVector() != null) {
      request.option("includeSortVector", options.includeSortVector());
    }
    var response = postToFixture(MAIN, request.json());

    assertIds(response, "m05", "m03", "m02", "m04", "m01");
    var statusKeys = new ArrayList<String>();
    if (Boolean.TRUE.equals(options.includeScores())) {
      statusKeys.add("documentResponses");
      assertScores(
          response,
          scores(3.25f, 0.93070626f, 3, null, rrf(3)),
          scores(2.125f, 0.8224976f, 4, null, rrf(4)),
          scores(1.5f, 0.9787127f, 1, null, rrf(1)),
          scores(0.5f, 0.9768599f, 2, null, rrf(2)),
          scores(-0.75f, 0.7665279f, 5, null, rrf(5)));
    }
    if (Boolean.TRUE.equals(options.includeSortVector())) {
      statusKeys.add("sortVector");
      assertSortVector(response, 0.1, 0.16, 0.31, 0.22, 0.15);
    }
    assertSuccessEnvelope(response, statusKeys.toArray(String[]::new));
    assertRerankerCalls(defaultModelCalls("ChatGPT upgraded", distinctPassages()));
  }

  static Stream<Named<ScoreOptions>> scoreOptions() {
    return Stream.of(
        Named.of("neither option -> only data, no status", new ScoreOptions(null, null)),
        Named.of("both options false -> only data, no status", new ScoreOptions(false, false)),
        Named.of("includeScores only -> only documentResponses", new ScoreOptions(true, null)),
        Named.of("includeSortVector only -> only sortVector", new ScoreOptions(null, true)),
        Named.of(
            "both options true -> documentResponses and sortVector, nothing else",
            new ScoreOptions(true, true)));
  }

  // Sends {"$hybrid": "A tree in the woods"} with both options. On HCD both reads run, so each
  // document has both ranks, $rrf adds them, and the one sortVector is the string's embedding; on
  // DSE $bm25Rank is null. The docs show scores with only $rerank and $vector; main returns five
  // keys and no BM25 score. The test pins current behavior; whether it is a bug is still under
  // discussion.
  @Test
  default void scoresAndSortVectorWhenBothReadsRun() {
    var request = findAndRerank().filter(DISTINCT_FILTER).hybrid("A tree in the woods");
    var response = postToFixture(MAIN, withScoresAndSortVector(request));

    assertIds(response, "m05", "m03", "m02", "m04", "m01");
    assertSuccessEnvelope(response, "documentResponses", "sortVector");
    assertNoDollarFields(response);
    assertScores(
        response,
        scores(3.25f, 0.942786f, 2, onHcd(5), rrf(2, onHcd(5))),
        scores(2.125f, 0.86899745f, 5, onHcd(4), rrf(5, onHcd(4))),
        scores(1.5f, 0.9312073f, 3, onHcd(2), rrf(3, onHcd(2))),
        scores(0.5f, 0.9721136f, 1, onHcd(3), rrf(1, onHcd(3))),
        scores(-0.75f, 0.87512046f, 4, onHcd(1), rrf(4, onHcd(1))));
    assertSortVector(response, 0.25, 0.25, 0.25, 0.25, 0.25);
    assertRerankerCalls(defaultModelCalls("A tree in the woods", distinctPassages()));
  }

  // On HCD, reranks a $lexical plus $vector sort on $lexical with includeScores. b5, found only by
  // the BM25 read, gets $vector -2147483648 and a null $vectorRank; b8, found only by the vector
  // read, gets a null $bm25Rank. The docs do not specify this; the test pins current behavior so
  // that any change is visible in review.
  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  default void scoresOfDocumentsFoundByOnlyOneRead() {
    var response =
        postToFixture(
            BYO,
            findAndRerank()
                .sort(
                    "{\"$hybrid\": {\"$lexical\": \"house hill grassy\", \"$vector\": %s}}"
                        .formatted(EXAMPLE_VECTOR))
                .options("{\"rerankOn\": \"$lexical\", \"rerankQuery\": \"house hill grassy\"}")
                .option("includeScores", true)
                .json());

    assertIds(response, "b5", "o4", "o3", "b9", "o2", "b8", "o1");
    assertSuccessEnvelope(response, "documentResponses");
    assertScores(
        response,
        scores(4.25f, (float) Integer.MIN_VALUE, null, 1, rrf(1)),
        scores(3.5f, 0.8039639f, 5, 3, rrf(5, 3)),
        scores(2.25f, 0.9996167f, 1, 5, rrf(1, 5)),
        scores(1.75f, 0.98955715f, 2, 6, rrf(2, 6)),
        scores(1.0f, 0.5678263f, 8, 2, rrf(8, 2)),
        scores(0.125f, 0.6669121f, 7, null, rrf(7)),
        scores(-0.5f, 0.9535399f, 3, 4, rrf(3, 4)));
    var sent = passages(BYO, "$lexical", "o1", "o2", "o3", "o4", "b5", "b8", "b9");
    assertRerankerCalls(defaultModelCalls("house hill grassy", sent));
  }

  // The reranker returns the logit 0.1234567891 for every passage. Expects each $rerank written as
  // 0.12345679, the nearest float, and the tied documents in RRF order (here the vector order).
  // The docs do not specify the precision; the test pins current behavior so that any change is
  // visible in review.
  @Test
  default void rerankScoreHasFloatPrecision() {
    String query = FakeRerankerModes.mode(Key.LOGIT, "0.1234567891");
    FakeRerankerClient.stubMode(query);
    var request = findAndRerank().filter(DISTINCT_FILTER).sort(CHATGPT_SORT);
    request.option("rerankQuery", query).option("includeScores", true);
    var response = postToFixture(MAIN, request.json());

    assertIds(response, "m02", "m04", "m05", "m03", "m01");
    assertSuccessEnvelope(response, "documentResponses");
    assertScores(
        response,
        scores(0.12345679f, 0.9787127f, 1, null, rrf(1)),
        scores(0.12345679f, 0.9768599f, 2, null, rrf(2)),
        scores(0.12345679f, 0.93070626f, 3, null, rrf(3)),
        scores(0.12345679f, 0.8224976f, 4, null, rrf(4)),
        scores(0.12345679f, 0.7665279f, 5, null, rrf(5)));
    var rerankText =
        Pattern.compile("\"\\$rerank\":([^,}]+)").matcher(response.extract().asString());
    assertThat(rerankText.results().map(m -> m.group(1))).hasSize(5).containsOnly("0.12345679");
    assertRerankerCalls(defaultModelCalls(query, distinctPassages()));
  }

  // Sends includeSortVector, rerankQuery and rerankOn but no sort. Expects UNEXPECTED_SERVER_ERROR
  // from buildVectorRead, so no response ever has a null sortVector. The docs do not mark sort as
  // optional; main gives a server error, not a request error. The test pins current behavior;
  // whether it is a bug is still under discussion.
  @Test
  default void includeSortVectorWithoutVectorSourceGivesServerError() {
    var response =
        postToFixture(
            MAIN,
            findAndRerank()
                .option("includeSortVector", true)
                .option("rerankQuery", "A tree in the woods")
                .option("rerankOn", "title")
                .json());

    assertApiError(
        response,
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "Error Class: IllegalArgumentException",
        "buildVectorRead() - no vector or vectorize");
  }

  /** A request that fails, sent with includeScores and includeSortVector, and its one error. */
  record FailingRequest(
      Fixture fixture,
      Builder request,
      Map<String, Object> headers,
      int httpStatus,
      ErrorCode<?> code,
      String title,
      String messageSnippet) {}

  // Sends failing requests with includeScores and includeSortVector. Expects HTTP 200 (401 without
  // a token) and one error with exactly id (a UUID), family, scope, errorCode, title (fixed per
  // code) and message; family and scope match the code's class; no data, status or reranker call.
  // The projection case has no embedding key, so build errors come before embedding. The docs do
  // not specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankResponseCases#failingRequests")
  default void errorResponseHasOneErrorAndNoDataOrStatus(FailingRequest failing) {
    var json = withScoresAndSortVector(failing.request());
    var response = postToFixture(failing.fixture(), json, failing.headers());

    assertApiError(response, failing.httpStatus(), failing.code(), failing.messageSnippet());
    Map<String, Object> error = response.extract().jsonPath().getMap("errors[0]");
    assertThat(error)
        .containsOnlyKeys("id", "family", "scope", "errorCode", "title", "message")
        .containsEntry("title", failing.title());
    assertThat(UUID.fromString((String) error.get("id"))).as("error id").isNotNull();
  }

  static Stream<Named<FailingRequest>> failingRequests() {
    var noEmbeddingKey = headers(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, null);
    return Stream.of(
        buildError(
            "build phase: $vector sort without rerankOn -> MISSING_RERANK_ON",
            MAIN,
            findAndRerank()
                .sort("{\"$hybrid\": {\"$vector\": [0.1, 0.16, 0.31, 0.22, 0.15]}}")
                .option("rerankQuery", "A tree in the woods"),
            RequestException.Code.MISSING_RERANK_ON,
            "Rerank field is missing",
            "does not specify which document field to rerank on"),
        buildError(
            "build phase: collection without vector -> UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION",
            NOVECTOR,
            findAndRerank()
                .sort("{\"$hybrid\": {\"$vector\": [0.1, 0.2, 0.3]}}")
                .options("{\"rerankOn\": \"title\", \"rerankQuery\": \"A tree in the woods\"}"),
            SortException.Code.UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION,
            "Vector sorting not supported by the collection",
            "does not have vectors enabled"),
        buildError(
            "build phase: collection without lexical -> LEXICAL_NOT_ENABLED_FOR_COLLECTION",
            NOLEX,
            findAndRerank()
                .sort("{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\", \"$lexical\": \"x\"}}"),
            SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION,
            "Lexical search is not enabled for the collection",
            "without a lexical index"),
        Named.of(
            "build phase: $similarity in projection -> UNSUPPORTED_PROJECTION_PARAM",
            new FailingRequest(
                MAIN,
                findAndRerank().sort(CHATGPT_SORT).projection("{\"$similarity\": 1}"),
                noEmbeddingKey,
                200,
                ProjectionException.Code.UNSUPPORTED_PROJECTION_PARAM,
                "Unsupported projection parameter",
                "(path: '$similarity')")),
        Named.of(
            "before the command: no Token header -> MISSING_AUTHENTICATION_TOKEN, HTTP 401",
            new FailingRequest(
                MAIN,
                findAndRerank().sort(CHATGPT_SORT),
                headers(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, null),
                401,
                APISecurityException.Code.MISSING_AUTHENTICATION_TOKEN,
                "Missing authorization token",
                "did not include an authorization token")),
        Named.of(
            "execution phase: embedding call fails -> UNEXPECTED_SERVER_ERROR, no rerank",
            new FailingRequest(
                MAIN,
                findAndRerank().sort(CHATGPT_SORT),
                noEmbeddingKey,
                200,
                ServerException.Code.UNEXPECTED_SERVER_ERROR,
                "Unexpected server error",
                "Error Message: Invalid API Key")));
  }

  /** A request error found while the command is built: default headers, HTTP 200, no rerank. */
  private static Named<FailingRequest> buildError(
      String name, Fixture fixture, Builder request, ErrorCode<?> code, String title, String text) {
    return Named.of(name, new FailingRequest(fixture, request, headers(), 200, code, title, text));
  }

  private static String withScoresAndSortVector(Builder request) {
    return request.option("includeScores", true).option("includeSortVector", true).json();
  }

  private static String[] distinctPassages() {
    return passages(MAIN, "$vectorize", "m01", "m02", "m03", "m04", "m05");
  }

  /** The rank on HCD; null on DSE, where the BM25 read does not run. */
  private Integer onHcd(int rank) {
    return lexicalAvailable() ? rank : null;
  }
}
