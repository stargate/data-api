package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWritePartialSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.ErrorFamily;
import io.stargate.sgv2.jsonapi.exception.ErrorScope;
import java.util.List;
import java.util.Map;

/**
 * Assertions on findAndRerank responses.
 *
 * <p>Error assertions take the error code enum constant, for example {@code
 * SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION}, and also check the {@code family} and
 * {@code scope} of the exception class that declares it (its public {@code FAMILY} and {@code
 * SCOPE} constants). For {@code UNEXPECTED_SERVER_ERROR} pass {@code
 * ServerException.Code.UNEXPECTED_SERVER_ERROR} and the snippet {@code "Error Class: <name>"}.
 */
public final class FindAndRerankAssertions {

  /** The five keys of {@code status.documentResponses[i].scores}. */
  private static final List<String> SCORE_KEYS =
      List.of("$rerank", "$vector", "$vectorRank", "$bm25Rank", "$rrf");

  private FindAndRerankAssertions() {}

  /**
   * The response succeeded (HTTP 200, no {@code errors}) and {@code data.documents[*]._id} equals
   * {@code ids} exactly and in order. Use Integer, Boolean or Map values for non-string ids.
   */
  public static ValidatableResponse assertIds(ValidatableResponse response, Object... ids) {
    response.statusCode(200).body("errors", is(nullValue()));
    List<Object> actual = response.extract().jsonPath().getList("data.documents._id");
    assertThat(actual).as("data.documents[*]._id").containsExactly(ids);
    return response;
  }

  /**
   * A success response has only {@code data}, plus {@code status} with exactly {@code statusKeys}
   * when any are given (so no warnings); {@code data} has only {@code documents} and a {@code
   * nextPageState} that is present and null.
   */
  @SuppressWarnings("unchecked")
  public static void assertSuccessEnvelope(ValidatableResponse response, String... statusKeys) {
    Map<String, Object> body = response.extract().jsonPath().getMap("$");
    assertThat(body)
        .as("response keys")
        .containsOnlyKeys(
            statusKeys.length == 0 ? new String[] {"data"} : new String[] {"data", "status"});
    assertThat((Map<String, Object>) body.get("data"))
        .as("data")
        .containsOnlyKeys("documents", "nextPageState")
        .containsEntry("nextPageState", null);
    if (statusKeys.length > 0) {
      assertThat((Map<String, Object>) body.get("status"))
          .as("status")
          .containsOnlyKeys(statusKeys);
    }
  }

  /** No returned document has a field whose name starts with {@code $}. */
  public static void assertNoDollarFields(ValidatableResponse response) {
    List<Map<String, Object>> documents = response.extract().jsonPath().getList("data.documents");
    assertThat(documents)
        .allSatisfy(
            doc ->
                assertThat(doc.keySet())
                    .as("fields of %s", doc.get("_id"))
                    .noneMatch(key -> key.startsWith("$")));
  }

  /** {@code status.sortVector} has exactly these values, compared as floats within 1e-6. */
  public static void assertSortVector(ValidatableResponse response, double... expected) {
    List<Number> actual = response.extract().jsonPath().getList("status.sortVector");
    assertThat(actual).as("status.sortVector").hasSize(expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertThat(actual.get(i).floatValue())
          .as("status.sortVector[%d]", i)
          .isCloseTo((float) expected[i], within(1e-6f));
    }
  }

  /** Expected scores of one document; a null value means the JSON value must be null. */
  public record ExpectedScores(
      Float rerank, Float vector, Integer vectorRank, Integer bm25Rank, Float rrf) {}

  /** Shortcut for {@code new ExpectedScores(...)}. */
  public static ExpectedScores scores(
      Float rerank, Float vector, Integer vectorRank, Integer bm25Rank, Float rrf) {
    return new ExpectedScores(rerank, vector, vectorRank, bm25Rank, rrf);
  }

  /**
   * The expected {@code $rrf}, computed as main does: the float sum of 1 / (60 + rank) over the
   * reads that found the document. Pass the vector rank and the BM25 rank; a null rank (the read
   * did not find the document, or did not run) adds nothing.
   */
  public static float rrf(Integer... ranks) {
    float sum = 0;
    for (Integer rank : ranks) {
      if (rank != null) {
        sum += (float) (1.0 / (60 + rank));
      }
    }
    return sum;
  }

  /**
   * {@code status.documentResponses} has one entry per expected document, in order, and each entry
   * is {@code {"scores": {...}}} with exactly the five score keys. Values are compared as floats
   * within 1e-6, so the special $vector value -2147483648 must match exactly.
   */
  public static void assertScores(ValidatableResponse response, ExpectedScores... perDocument) {
    List<Map<String, Object>> entries =
        response.extract().jsonPath().getList("status.documentResponses");
    assertThat(entries).as("status.documentResponses").hasSize(perDocument.length);
    for (int i = 0; i < perDocument.length; i++) {
      assertThat(entries.get(i)).as("documentResponses[%d]", i).containsOnlyKeys("scores");
      @SuppressWarnings("unchecked")
      Map<String, Object> scores = (Map<String, Object>) entries.get(i).get("scores");
      assertThat(scores.keySet())
          .as("documentResponses[%d].scores keys", i)
          .hasSameElementsAs(SCORE_KEYS);
      ExpectedScores e = perDocument[i];
      Object[] expected = {e.rerank(), e.vector(), e.vectorRank(), e.bm25Rank(), e.rrf()};
      for (int k = 0; k < SCORE_KEYS.size(); k++) {
        Object actual = scores.get(SCORE_KEYS.get(k));
        String what = "documentResponses[%d].scores.%s".formatted(i, SCORE_KEYS.get(k));
        if (expected[k] == null) {
          assertThat(actual).as(what).isNull();
        } else {
          assertThat(actual).as(what).isInstanceOf(Number.class);
          assertThat(((Number) actual).floatValue())
              .as(what)
              .isCloseTo(((Number) expected[k]).floatValue(), within(1e-6f));
        }
      }
    }
  }

  /**
   * A request error: HTTP 200, a single error with this code, the family and scope of its exception
   * class, and a message that contains every snippet and no unreplaced {@code ${...}}; no {@code
   * data} and no {@code status}; and the reranker was not called.
   */
  public static void assertApiError(
      ValidatableResponse response, ErrorCode<?> code, String... messageSnippets) {
    assertApiError(response, 200, code, messageSnippets);
  }

  /** Like the {@code assertApiError} without a status argument, but for this HTTP status. */
  public static void assertApiError(
      ValidatableResponse response, int httpStatus, ErrorCode<?> code, String... messageSnippets) {
    response.statusCode(httpStatus).body("$", responseIsError());
    assertSingleErrorAt(response, "errors", code, messageSnippets);
    RerankerAssertions.assertRerankerNotCalled();
  }

  /**
   * An error after the reranker may have been called: the checks of {@link
   * #assertApiError(ValidatableResponse, ErrorCode, String...)}, except that the reranker must have
   * received exactly {@code rerankerCalls} requests, retries included.
   */
  public static void assertRuntimeError(
      ValidatableResponse response,
      ErrorCode<?> code,
      int rerankerCalls,
      String... messageSnippets) {
    response.statusCode(200).body("$", responseIsError());
    assertSingleErrorAt(response, "errors", code, messageSnippets);
    RerankerAssertions.assertRerankerCallCount(rerankerCalls);
  }

  /**
   * An insert that failed for its document while it was shredded: HTTP 200, {@code errors} and
   * {@code status} but no {@code data}, an empty {@code status.insertedIds}, the single error as in
   * {@link #assertApiError(ValidatableResponse, ErrorCode, String...)}, and no reranker call.
   */
  public static void assertWriteError(
      ValidatableResponse response, ErrorCode<?> code, String... messageSnippets) {
    response
        .statusCode(200)
        .body("$", responseIsWritePartialSuccess())
        .body("status.insertedIds", empty());
    assertSingleErrorAt(response, "errors", code, messageSnippets);
    RerankerAssertions.assertRerankerNotCalled();
  }

  /**
   * The array at {@code errorsPath}, for example {@code "errors"} or {@code
   * "result.structuredContent.errors"} of an MCP tool response, has exactly one error, with this
   * code, the family and scope of its exception class, and a message that contains every snippet
   * and no unreplaced {@code ${...}}. Checks nothing else.
   */
  public static void assertSingleErrorAt(
      ValidatableResponse response,
      String errorsPath,
      ErrorCode<?> code,
      String... messageSnippets) {
    Class<?> exceptionClass = ((Enum<?>) code).getDeclaringClass().getEnclosingClass();
    String error = errorsPath + "[0].";
    response
        .body(errorsPath, hasSize(1))
        .body(error + "errorCode", is(code.name()))
        .body(error + "family", is(family(exceptionClass)))
        .body(error + "scope", is(scope(exceptionClass)))
        .body(error + "message", not(containsString("${")));
    for (String snippet : messageSnippets) {
      response.body(error + "message", containsString(snippet));
    }
  }

  private static String family(Class<?> exceptionClass) {
    try {
      return ((ErrorFamily) exceptionClass.getField("FAMILY").get(null)).name();
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("No FAMILY constant on " + exceptionClass, e);
    }
  }

  private static String scope(Class<?> exceptionClass) {
    try {
      return ((ErrorScope) exceptionClass.getField("SCOPE").get(null)).scope();
    } catch (NoSuchFieldException e) {
      return ""; // RequestException and ServerException have no scope
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Cannot read SCOPE of " + exceptionClass, e);
    }
  }
}
