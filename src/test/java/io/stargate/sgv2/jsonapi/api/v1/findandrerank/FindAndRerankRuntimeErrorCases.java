package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.*;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;

/**
 * findAndRerank failures after the request passed validation: the embedding step fails, the
 * database rejects the read, or the reranker answers with an error status, a timeout, or a reply
 * that cannot be used. Also the extra reranking models of the test configuration: a bad URL, a
 * negative batch size, a short timeout, no retries, a growing back-off, a capped back-off, and a
 * name that is listed twice. The tests run as methods of {@code
 * FindAndRerankFakeRerankerIntegrationTest}.
 *
 * <p>Most tests send one request to frr_main, sorted by the vectorize text "ChatGPT upgraded" and
 * filtered to m01 to m05, so the reranker gets one batch of five passages; {@code rerankQuery}
 * carries a {@link FakeRerankerModes} mode, which {@code postMain} registers. Without the filter,
 * twelve passages go out as batches of 10 and 2.
 *
 * <p>For review: the docs say almost nothing about these failures, so most tests pin current
 * behavior. Request counts include retries: by default a timeout (HTTP 408, HTTP 504 or a read
 * timeout) is retried 3 times and every other failure is sent once. Server errors are checked by
 * the "Error Class" of the message; the classes of the parse errors come from the Quarkus REST
 * client, so a client upgrade may change them. The default-model timeout cases take about 6 and 21
 * seconds. One test rewrites a table comment with CQL and restores it; it depends on the internal
 * comment format.
 */
public interface FindAndRerankRuntimeErrorCases extends FindAndRerankTestContext {

  String RUNTIME_ERROR_FACTORIES =
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRuntimeErrorCases#";

  /** Keeps m01 to m05 of the main collection: the reranker gets one batch of five passages. */
  String FIVE_PASSAGES_FILTER = "{\"grp\": \"distinct\"}";

  /** A $hybrid object without $lexical runs only the vector read, on HCD and on DSE. */
  String UPGRADED_VECTORIZE_SORT = "{\"$hybrid\": {\"$vectorize\": \"ChatGPT upgraded\"}}";

  /** m01 to m05 by their rerank scores (3.25, 2.125, 1.5, 0.5, -0.75). */
  List<String> RERANK_ORDER_M01_TO_M05 = List.of("m05", "m03", "m02", "m04", "m01");

  // Sends a $vectorize sort without x-embedding-api-key, or with a wrong one. Expects
  // UNEXPECTED_SERVER_ERROR from the test embedding provider's RuntimeException, no data and no
  // reranker request: a failed embedding stops the command before the reads and the rerank.
  // The docs do not cover this case and main answers with a generic server error. The test pins
  // current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "embeddingKeyCases")
  default void embeddingKeyFailureStopsCommandBeforeReadsAndRerank(Map<String, Object> headers) {
    assertApiError(
        postMain(headers),
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        "Error Class: RuntimeException",
        "Error Message: Invalid API Key");
  }

  static Stream<Named<Map<String, Object>>> embeddingKeyCases() {
    return Stream.of(
        Named.of("no x-embedding-api-key header", headers("x-embedding-api-key", null)),
        Named.of("wrong x-embedding-api-key value", headers("x-embedding-api-key", "wrong-key")));
  }

  // Sends reranking-api-key as spaces or as an empty value. Expects
  // RERANKING_PROVIDER_AUTHENTICATION_KEY_NOT_PROVIDED and no reranker request: the blank key is
  // not replaced by the Data API token and is rejected before the first batch is sent. The docs
  // do not specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "blankRerankingKeyCases")
  default void blankRerankingApiKeyIsRejectedBeforeFirstBatch(String headerValue) {
    assertApiError(
        postMain(headers("reranking-api-key", headerValue)),
        SchemaException.Code.RERANKING_PROVIDER_AUTHENTICATION_KEY_NOT_PROVIDED,
        "No authentication key provided for reranking provider");
  }

  static Stream<Named<String>> blankRerankingKeyCases() {
    return Stream.of(
        Named.of("reranking-api-key is three spaces", "   "),
        Named.of("reranking-api-key is empty", ""));
  }

  // Sorts a cosine collection by the zero vector with includeScores, the obvious way to ask for a
  // similarity that is not finite. Expects INVALID_DATABASE_QUERY from the read and no reranker
  // request, so the "score must be finite" check on similarities is never reached. The docs do
  // not specify this; the test pins current behavior so that any change is visible in review.
  @Test
  default void zeroQueryVectorIsRejectedByTheDatabaseRead() {
    assertApiError(
        postToFixture(
            BYO,
            findAndRerank()
                .sort("{\"$hybrid\": {\"$vector\": [0.0, 0.0, 0.0]}}")
                .options("{\"includeScores\":true, \"rerankOn\":\"title\", \"rerankQuery\":\"q\"}")
                .json()),
        DatabaseException.Code.INVALID_DATABASE_QUERY,
        "vectors cannot be indexed or queried with cosine similarity");
  }

  // Sends a $vectorize sort to a collection whose custom embedding service has 4 dimensions; the
  // test embedding provider only has vectors for 5 and 6, so it returns none. Expects
  // UNEXPECTED_SERVER_ERROR with the vector count mismatch and no reranker request.
  // The docs do not cover this case and main answers with a generic server error. The test pins
  // current behavior; whether it is a bug is still under discussion.
  @Test
  default void customEmbeddingWithoutVectorsForDimensionGivesServerError() {
    String collection = ensure(TMP_DIM4);
    try {
      assertApiError(
          postToCollection(collection, findAndRerank().sort(UPGRADED_VECTORIZE_SORT).json()),
          ServerException.Code.UNEXPECTED_SERVER_ERROR,
          "Error Class: IllegalStateException",
          "Size of returned vectors and waiting EmbeddingActions do not match");
    } finally {
      postToKeyspace("{\"deleteCollection\": {\"name\": \"" + collection + "\"}}").statusCode(200);
      createdFixtures().remove(collection);
    }
  }

  // Rewrites the table comment so that the collection's vectorize provider no longer exists, then
  // sends a $vectorize request, a $vector request, and a $vector request without rerankOn (a
  // check that normally fails while the command is built). Expects EMBEDDING_PROVIDER_UNAVAILABLE
  // for all three and no reranker request: the embedding client is created for every command on
  // a vectorize collection, before any findAndRerank check. The docs do not specify this; the test
  // pins current behavior so that any change is visible in review.
  @Test
  default void missingEmbeddingProviderFailsEveryFindAndRerankFirst() {
    String collection = ensure(COMMENT);
    String vectorizeRequest = findAndRerank().sort(UPGRADED_VECTORIZE_SORT).json();
    String vectorSort = "{\"$hybrid\": {\"$vector\": [0.1, 0.16, 0.31, 0.22, 0.15]}}";
    TableCommentRewriter.withRewrittenComment(
        keyspace(),
        collection,
        comment ->
            ((ObjectNode) comment.at("/collection/options/vector/service"))
                .put("provider", "no-such-provider"),
        () -> postToCollection(collection, vectorizeRequest),
        TableCommentRewriter::errorCode,
        response -> {
          FakeRerankerClient.reset();
          for (String request :
              List.of(
                  vectorizeRequest,
                  findAndRerank()
                      .sort(vectorSort)
                      .options("{\"rerankOn\": \"$vectorize\", \"rerankQuery\": \"q\"}")
                      .json(),
                  findAndRerank().sort(vectorSort).option("rerankQuery", "q").json())) {
            assertApiError(
                postToCollection(collection, request),
                EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNAVAILABLE,
                "The service provider 'no-such-provider' is not supported.");
          }
        });
  }

  // The reranker answers HTTP 200 with a reply that findAndRerank can use, in some cases only on
  // the retry after a timeout. Expects the documents in logit order (vector order when every
  // logit is missing, because all rerank scores are then 0) and exactly the listed requests, each
  // with all passages. The docs do not specify the reranker protocol; the test pins current
  // behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "acceptedReplyCases")
  default void usableRerankerReplyOrdersTheDocuments(
      String rerankQuery, String filter, int requests, List<String> ids) {
    assertReranked(postMain(filter, rerankQuery, null), rerankQuery, requests, ids);
  }

  static Stream<Arguments> acceptedReplyCases() {
    return Stream.of(
        accepted("valid NVIDIA reply -> ordered by logit, 1 request", "Rank them"),
        accepted(
            "Content-Type application/json; charset=utf-8 -> accepted", mode(Key.JSON_CHARSET)),
        accepted("usage without total_tokens -> accepted", mode(Key.PARTIAL_USAGE)),
        accepted(
            "usage without prompt_tokens -> accepted", mode(Key.PARTIAL_USAGE, "prompt_tokens")),
        accepted("unknown fields in the reply -> ignored", mode(Key.UNKNOWN_FIELDS)),
        accepted(
            "scores in \"score\" instead of \"logit\" -> all rerank scores 0, vector order",
            mode(Key.SCORE_FIELD),
            1,
            List.of("m02", "m04", "m05", "m03", "m01")),
        accepted(
            "HTTP 408 once, then a valid reply -> retried, 2 requests",
            mode(Key.STATUS_ONCE, 408),
            2,
            RERANK_ORDER_M01_TO_M05),
        accepted(
            "read timeout once, then a valid reply -> retried, 2 requests",
            mode(Key.DELAY_ONCE_MS, 5500),
            2,
            RERANK_ORDER_M01_TO_M05),
        Arguments.of(
            Named.of(
                "rankings without index, single passage -> index 0 is right, accepted",
                mode(Key.NO_INDEX)),
            "{\"_id\": \"m05\"}",
            1,
            List.of("m05")));
  }

  // The reranker answers with an HTTP error status. Expects RERANKING_PROVIDER_CLIENT_ERROR for
  // 4xx, RATE_LIMITED for 429, SERVER_ERROR for 5xx and UNEXPECTED_RESPONSE above 599, all sent
  // once; 408 and 504 give RERANKING_PROVIDER_TIMEOUT after 3 retries with a back-off. The message
  // quotes the "message" field of a JSON body (as a JSON string), a whole JSON body without one,
  // or a text body as is. The docs do not specify this; the test pins current behavior so that
  // any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "httpErrorCases")
  default void rerankerHttpErrorIsMappedToRerankingError(
      int httpStatus, Body body, ErrorCode<?> code, int requests) {
    String text = "FAKE_RERANKER_STATUS_" + httpStatus;
    String message =
        switch (body) {
          case JSON_MESSAGE -> "\"" + text + "\"";
          case JSON_NO_MESSAGE -> "{\"error\":\"" + text + "\"}";
          default -> text;
        };
    String snippet =
        code == RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT
            ? "The HTTP status code was: %d.\nThe error message was: %s."
            : "Provider: nvidia; HTTP Status: %d; Error Message: %s.";
    long start = System.nanoTime();
    var response = postMain(FIVE_PASSAGES_FILTER, status(httpStatus, body), null);
    long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
    assertRuntimeError(response, code, requests, snippet.formatted(httpStatus, message));
    // Each retry first waits at least the default initial-back-off-millis of 100 ms.
    assertThat(millis)
        .as("milliseconds until the error")
        .isGreaterThanOrEqualTo((requests - 1) * 100L);
  }

  static Stream<Arguments> httpErrorCases() {
    var json = Body.JSON_MESSAGE;
    var clientError = SchemaException.Code.RERANKING_PROVIDER_CLIENT_ERROR;
    var serverError = SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR;
    var timeout = RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT;
    var rateLimited = SchemaException.Code.RERANKING_PROVIDER_RATE_LIMITED;
    var unexpected = SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE;
    return Stream.of(
        httpError("HTTP 400 -> CLIENT_ERROR, 1 request", 400, json, clientError, 1),
        httpError("HTTP 401 -> CLIENT_ERROR, 1 request", 401, json, clientError, 1),
        httpError("HTTP 403 -> CLIENT_ERROR, 1 request", 403, json, clientError, 1),
        httpError("HTTP 404 -> CLIENT_ERROR, 1 request", 404, json, clientError, 1),
        httpError("HTTP 429 -> RATE_LIMITED, 1 request", 429, json, rateLimited, 1),
        httpError("HTTP 500 -> SERVER_ERROR, 1 request", 500, json, serverError, 1),
        httpError("HTTP 502 -> SERVER_ERROR, 1 request", 502, json, serverError, 1),
        httpError("HTTP 503 -> SERVER_ERROR, 1 request", 503, json, serverError, 1),
        httpError("HTTP 408 -> TIMEOUT after 3 retries, 4 requests", 408, json, timeout, 4),
        httpError("HTTP 504 -> TIMEOUT after 3 retries, 4 requests", 504, json, timeout, 4),
        httpError("HTTP 600 -> UNEXPECTED_RESPONSE, 1 request", 600, json, unexpected, 1),
        httpError(
            "HTTP 400, JSON body without message -> whole body in the message",
            400,
            Body.JSON_NO_MESSAGE,
            clientError,
            1),
        httpError(
            "HTTP 400, text body -> body text in the message", 400, Body.TEXT, clientError, 1));
  }

  // The reranker answers HTTP 200 with a Content-Type that is not JSON, without a Content-Type,
  // or without a body. Expects RERANKING_PROVIDER_UNEXPECTED_RESPONSE after one request, no retry.
  // The docs do not specify this; the test pins current behavior so that any change is visible in
  // review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "nonJsonReplyCases")
  default void rerankerReplyWithoutJsonContentIsRejected(String rerankQuery, String snippet) {
    assertRuntimeError(
        postMain(FIVE_PASSAGES_FILTER, rerankQuery, null),
        SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE,
        1,
        snippet);
  }

  static Stream<Arguments> nonJsonReplyCases() {
    return Stream.of(
        Arguments.of(
            Named.of("Content-Type text/plain -> rejected", mode(Key.CONTENT_TYPE, "text/plain")),
            "but found 'text/plain'; HTTP Status: 200"),
        Arguments.of(
            Named.of(
                "Content-Type application/xml -> rejected",
                mode(Key.CONTENT_TYPE, "application/xml")),
            "but found 'application/xml'"),
        Arguments.of(
            Named.of(
                "Content-Type application/problem+json -> rejected",
                mode(Key.CONTENT_TYPE, "application/problem+json")),
            "but found 'application/problem+json'"),
        Arguments.of(
            Named.of("no Content-Type header -> rejected", mode(Key.NO_CONTENT_TYPE)),
            "but found 'null'"),
        Arguments.of(
            Named.of("HTTP 200 without a body -> rejected", mode(Key.NO_BODY)),
            "No response body from the model provider"));
  }

  // The reranker answers HTTP 200 (or 301) with a body that cannot be read into scores, including
  // JSON sent as text/json (it passes the Content-Type check, but the REST client has no JSON
  // reader for it), or with indexes that do not match the passages. Expects UNEXPECTED_SERVER_ERROR
  // after one request per batch, no retry. The two-batch case shows that an index is not checked
  // against its own batch. The docs do not cover these replies and main answers with a generic
  // server error. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "unusableReplyCases")
  default void unusableRerankerReplyGivesServerError(
      String rerankQuery, int batches, String snippet) {
    assertRuntimeError(
        postMain(batches == 1 ? FIVE_PASSAGES_FILTER : null, rerankQuery, null),
        ServerException.Code.UNEXPECTED_SERVER_ERROR,
        batches,
        snippet);
  }

  static Stream<Arguments> unusableReplyCases() {
    String npe = "Error Class: NullPointerException";
    String sizeMismatch = "unrankedDocuments and ranks must be the same size";
    String mergedTwice = "Cannot merge two scores that both exist";
    // A truncated body, a field of the wrong type and an unreadable Content-Type all surface as
    // ProcessingException; only the duplicate key surfaces as ClientWebApplicationException.
    String duplicateKey = "Error Class: ClientWebApplicationException";
    String unreadable = "Error Class: ProcessingException";
    String truncated =
        "Error Class: ProcessingException\n"
            + "Error Message: com.fasterxml.jackson.databind.JsonMappingException:"
            + " Unexpected end-of-input";
    return Stream.of(
        unusable("reply without usage -> NullPointerException", mode(Key.NO_USAGE), npe),
        unusable("reply is {} -> NullPointerException", mode(Key.EMPTY_OBJECT), npe),
        unusable("reply without rankings -> NullPointerException", mode(Key.NO_RANKINGS), npe),
        unusable("rankings is [] -> size mismatch", mode(Key.EMPTY_RANKINGS), sizeMismatch),
        unusable("one ranking missing -> size mismatch", mode(Key.DROP_ONE), sizeMismatch),
        unusable("one ranking too many -> size mismatch", mode(Key.EXTRA_ONE), sizeMismatch),
        unusable("an index repeated -> merged twice", mode(Key.DUP_INDEX), mergedTwice),
        unusable(
            "index equal to the passage count -> out of bounds",
            mode(Key.INDEX_OUT_OF_RANGE),
            "rank index 5 out of bounds for unrankedDocuments.size()=5"),
        unusable(
            "index -1 -> out of bounds",
            mode(Key.NEGATIVE_INDEX),
            "rank index -1 out of bounds for unrankedDocuments.size()=5"),
        unusable("rankings without index -> merged twice", mode(Key.NO_INDEX), mergedTwice),
        Arguments.of(
            Named.of(
                "first batch returns index 10, which is in the second batch -> merged twice",
                mode(Key.INDEX_PLUS_IF_SIZE, "10,1")),
            2,
            mergedTwice),
        unusable("truncated JSON -> parse error", mode(Key.INVALID_JSON), truncated),
        unusable("rankings key twice -> parse error", mode(Key.DUP_KEY), duplicateKey),
        unusable("rankings is a string -> mapping error", mode(Key.WRONG_TYPES), unreadable),
        unusable(
            "Content-Type text/json -> no reader", mode(Key.CONTENT_TYPE, "text/json"), unreadable),
        unusable(
            "HTTP 301 redirect, not followed -> server error", mode(Key.REDIRECT), "Error Class: "),
        unusable(
            "logit 1e39 overflows float -> score must be finite",
            mode(Key.LOGIT, "1e39"),
            "Score must be finite, got Infinity"));
  }

  // Twelve passages go out as two concurrent batches and the fake fails at least one of them with
  // a status that is not retried. Expects the error of the batch that fails first and two
  // reranker requests; a delay fixes which failure comes first. The docs do not specify this; the
  // test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "failingBatchCases")
  default void firstFailingBatchFailsTheWholeCommand(
      String rerankQuery, ErrorCode<?> code, String snippet) {
    assertRuntimeError(postMain(null, rerankQuery, null), code, 2, snippet);
  }

  static Stream<Arguments> failingBatchCases() {
    return Stream.of(
        Arguments.of(
            Named.of(
                "one batch HTTP 500, the other HTTP 200 -> SERVER_ERROR",
                combine(
                    mode(Key.FAIL_IF_CONTAINS, "A recipe for spiced pumpkin soup"),
                    mode(Key.DELAY_MS, 500))),
            SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR,
            "HTTP Status: 500"),
        Arguments.of(
            Named.of(
                "HTTP 400 arrives before the other batch's HTTP 500 -> CLIENT_ERROR",
                combine(mode(Key.FAIL_BOTH, "500,400"), mode(Key.DELAY_ONCE_MS, 1000))),
            SchemaException.Code.RERANKING_PROVIDER_CLIENT_ERROR,
            "HTTP Status: 400"));
  }

  // The reranker answers too late for the model's read timeout, or with HTTP 408, every time.
  // Expects RERANKING_PROVIDER_TIMEOUT after 1 + at-most-retries requests to that model, and
  // timings that show the model's settings took effect: 4 tries at the default 5 s timeout take at
  // least 18 s; with a 200 ms back-off that doubles, the requests reach the fake at least 150, 350
  // and 600 ms apart (each wait is at least its back-off of 200, 400 and 800 ms; without growth
  // each is about 200 ms); 6 retries with a back-off capped at 150 ms take under 4 s (6.3 s if
  // uncapped). A model with 0 retries still sends one request. After a read timeout the message
  // gives <CLIENT SOCKET TIMEOUT> as both the HTTP status and the error message. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "timeoutCases")
  default void rerankerTimeoutIsRetriedAsTheModelConfigures(
      Model model,
      String rerankQuery,
      int requests,
      String snippet,
      int minSeconds,
      int maxSeconds,
      List<Long> minGapsMillis) {
    Model override = model == Model.DEFAULT ? null : model;
    long start = System.nanoTime();
    var response = postMain(FIVE_PASSAGES_FILTER, rerankQuery, override);
    long seconds = Duration.ofNanos(System.nanoTime() - start).toSeconds();
    assertRuntimeError(
        response,
        RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT,
        requests,
        "The reranking provider was: nvidia.",
        snippet);
    assertRerankerCallCount(model, requests);
    List<RecordedRerankRequest> tries = FakeRerankerClient.requests(model);
    for (int i = 0; i < minGapsMillis.size(); i++) {
      long gap = tries.get(i + 1).arrivalMillis() - tries.get(i).arrivalMillis();
      assertThat(gap)
          .as("milliseconds between request %d and %d", i + 1, i + 2)
          .isGreaterThanOrEqualTo(minGapsMillis.get(i));
    }
    assertThat(seconds)
        .as("seconds until the error")
        .isGreaterThanOrEqualTo(minSeconds)
        .isLessThan(maxSeconds);
  }

  static Stream<Arguments> timeoutCases() {
    String socketTimeout =
        "The HTTP status code was: <CLIENT SOCKET TIMEOUT>.\n"
            + "The error message was: <CLIENT SOCKET TIMEOUT>.";
    int noMax = Integer.MAX_VALUE;
    return Stream.of(
        Arguments.of(
            Named.of("default model, reply after 5.5 s -> 4 requests", Model.DEFAULT),
            mode(Key.DELAY_MS, 5500),
            4,
            socketTimeout,
            18,
            noMax,
            List.of()),
        Arguments.of(
            Named.of("500 ms timeout, 1 retry, reply after 1 s -> 2 requests", Model.FAST_TIMEOUT),
            mode(Key.DELAY_MS, 1000),
            2,
            socketTimeout,
            0,
            noMax,
            List.of()),
        Arguments.of(
            Named.of("model with at-most-retries 0, HTTP 408 -> 1 request", Model.ZERO_RETRIES),
            status(408, Body.JSON_MESSAGE),
            1,
            "The HTTP status code was: 408.",
            0,
            noMax,
            List.of()),
        Arguments.of(
            Named.of(
                "back-off 200 ms doubling, no jitter, HTTP 408 -> 4 requests, at least 1 s",
                Model.GROWING_BACK_OFF),
            status(408, Body.JSON_MESSAGE),
            4,
            "The HTTP status code was: 408.",
            1,
            noMax,
            List.of(150L, 350L, 600L)),
        Arguments.of(
            Named.of(
                "back-off 100 ms capped at 150 ms, no jitter, HTTP 408 -> 7 requests, under 4 s",
                Model.CAPPED_BACK_OFF),
            status(408, Body.JSON_MESSAGE),
            7,
            "The HTTP status code was: 408.",
            0,
            4,
            List.of()));
  }

  // Overrides the reranker with a test model whose URL or batch size is broken. Expects
  // UNEXPECTED_SERVER_ERROR, no request to the fake and the error in under 3 s: the invalid URI
  // and the negative batch size fail before sending, the refused connection and the unknown host
  // fail without a retry. Those two models wait 3 s without jitter before each retry, so one retry
  // would take at least 3 s. The docs do not cover these settings and main answers with a generic
  // server error. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(RUNTIME_ERROR_FACTORIES + "brokenModelCases")
  default void brokenRerankingModelSettingGivesServerError(Model model, String snippet) {
    ensure(MAIN); // creating the collection is not part of the timing
    long start = System.nanoTime();
    var response = postMain(FIVE_PASSAGES_FILTER, "ChatGPT upgraded", model);
    long millis = Duration.ofNanos(System.nanoTime() - start).toMillis();
    assertRuntimeError(response, ServerException.Code.UNEXPECTED_SERVER_ERROR, 0, snippet);
    assertThat(millis).as("milliseconds until the error").isLessThan(3000L);
  }

  static Stream<Arguments> brokenModelCases() {
    return Stream.of(
        Arguments.of(
            Named.of("url with a space -> invalid URI", Model.BAD_URL),
            "Error Class: IllegalArgumentException"),
        Arguments.of(
            Named.of("max-batch-size -1 -> no batch can be cut", Model.NEGATIVE_BATCH),
            "fromIndex(0) > toIndex(-1)"),
        Arguments.of(
            Named.of("url port not listening -> connection refused", Model.CONNECTION_REFUSED),
            "Connection refused"),
        Arguments.of(
            Named.of("url host does not resolve -> unknown host", Model.UNKNOWN_HOST),
            "farr-no-such-host.invalid"));
  }

  // Overrides the reranker with the default model's name, which the test configuration lists a
  // second time with another URL. Expects the normal result and one request on the first entry's
  // (the default model's) URL only: the lookup by name takes the first match. The docs do not
  // specify this; the test pins current behavior so that any change is visible in review.
  @Test
  default void duplicateModelNameUsesTheFirstConfiguredEntry() {
    var response = postMain(FIVE_PASSAGES_FILTER, "ChatGPT upgraded", Model.DUPLICATE);
    assertReranked(response, "ChatGPT upgraded", 1, RERANK_ORDER_M01_TO_M05);
  }

  // The test configuration sets the middle value of hybrid-search-vector-limit and
  // hybrid-search-lexical-limit to 5. Sends a vector-only sort, and on HCD also a $hybrid string
  // that runs both reads, without hybridLimits. Expects every read in the full trace to use LIMIT
  // 50: the configured default is not used. The findAndRerank docs (Parameters, hybridLimits) say
  // the default is the value of limit; main reads 50 and ignores this setting. The test pins
  // current behavior; whether it is a bug is still under discussion.
  @Test
  default void configuredHybridLimitDefaultIsNotUsedByTheReads() {
    assertEveryReadUsesLimit50(UPGRADED_VECTORIZE_SORT);
    Assumptions.assumeTrue(lexicalAvailable(), "the BM25 read needs lexical (HCD)");
    assertEveryReadUsesLimit50("{\"$hybrid\": \"ChatGPT upgraded\"}");
  }

  private void assertEveryReadUsesLimit50(String sort) {
    FakeRerankerClient.reset();
    var response =
        postToFixture(
            MAIN,
            findAndRerank().filter(FIVE_PASSAGES_FILTER).sort(sort).json(),
            headers("Feature-Flag-request-tracing-full", "true"));
    assertReranked(response, "ChatGPT upgraded", 1, RERANK_ORDER_M01_TO_M05);
    String trace = response.extract().asString();
    assertThat(Pattern.compile("LIMIT (\\d+)").matcher(trace).results().map(m -> m.group(1)))
        .as("LIMIT values in status.trace")
        .isNotEmpty()
        .containsOnly("50");
  }

  /** Registers the mode, then posts {@link #mainRequest} to the main collection. */
  private ValidatableResponse postMain(String filter, String rerankQuery, Model override) {
    FakeRerankerClient.stubMode(rerankQuery);
    return postToFixture(MAIN, mainRequest(filter, rerankQuery, override));
  }

  /** Like {@code postMain(FIVE_PASSAGES_FILTER, "ChatGPT upgraded", null)}, with these headers. */
  private ValidatableResponse postMain(Map<String, Object> headers) {
    return postToFixture(
        MAIN, mainRequest(FIVE_PASSAGES_FILTER, "ChatGPT upgraded", null), headers);
  }

  /** The command JSON for the main collection; a null filter keeps all twelve passages. */
  private static String mainRequest(String filter, String rerankQuery, Model override) {
    var request = findAndRerank().sort(UPGRADED_VECTORIZE_SORT).option("rerankQuery", rerankQuery);
    if (filter != null) {
      request.filter(filter);
    }
    if (override != null) {
      request.option(
          "rerank", Map.of("provider", override.provider(), "modelName", override.modelName()));
    }
    return request.json();
  }

  /**
   * The response has exactly {@code ids} in order, and the default model got {@code requests}
   * requests with this query whose passages, together, are the {@code $vectorize} texts of {@code
   * ids} once per request.
   */
  private static void assertReranked(
      ValidatableResponse response, String query, int requests, List<String> ids) {
    assertIds(response, ids.toArray());
    String[] passageIds =
        Collections.nCopies(requests, ids).stream().flatMap(List::stream).toArray(String[]::new);
    assertRerankerCalls(
        calls(requests).query(query).passages(passages(MAIN, "$vectorize", passageIds)));
  }

  /** One request, documents in logit order. */
  private static Arguments accepted(String description, String rerankQuery) {
    return accepted(description, rerankQuery, 1, RERANK_ORDER_M01_TO_M05);
  }

  private static Arguments accepted(
      String description, String rerankQuery, int requests, List<String> ids) {
    return Arguments.of(Named.of(description, rerankQuery), FIVE_PASSAGES_FILTER, requests, ids);
  }

  private static Arguments httpError(
      String description, int httpStatus, Body body, ErrorCode<?> code, int requests) {
    return Arguments.of(Named.of(description, httpStatus), body, code, requests);
  }

  private static Arguments unusable(String description, String rerankQuery, String snippet) {
    return Arguments.of(Named.of(description, rerankQuery), 1, snippet);
  }
}
