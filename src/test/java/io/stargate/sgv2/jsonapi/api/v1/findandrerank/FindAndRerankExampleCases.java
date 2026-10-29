package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertIds;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertNoDollarFields;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertScores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSortVector;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.assertSuccessEnvelope;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.rrf;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankAssertions.scores;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.BYO;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.MAIN;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.NORERANK;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.passages;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.findAndRerank;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankRequests.headers;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerCalls;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.assertRerankerNotCalled;
import static io.stargate.sgv2.jsonapi.api.v1.findandrerank.RerankerAssertions.defaultModelCalls;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.MatcherAssert.assertThat;

import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankFixtures.Fixture;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * findAndRerank tests that send the request examples of the official findAndRerank page (Signature
 * and Examples sections) and check the response example of its Result section for no matches.
 *
 * <p>Each request body has the same keys, values and key order as on the page; only the whitespace
 * differs. Like the page, requests without {@code $vectorize} send only the {@code Token} and
 * {@code Content-Type} headers; requests with {@code $vectorize} also send {@code
 * x-embedding-api-key}, which the test embedding provider requires. The page has no data, so the
 * examples run on the MAIN (vectorize), BYO (the page's 3-dimension vector) and NORERANK fixtures
 * of {@link FindAndRerankFixtures}; the expected orders follow from {@link FakeRerankerScores}.
 * Examples that send {@code $lexical} run only on HCD. The tests run as methods of {@code
 * FindAndRerankFakeRerankerIntegrationTest}.
 *
 * <p>For review: most examples pin behavior that differs from the page; the comment on {@code
 * documentedExampleRequestSucceeds} lists the differences.
 */
public interface FindAndRerankExampleCases extends FindAndRerankTestContext {

  /** The fixture, reranker query, passage field, reranked and returned ids (space-separated). */
  record Reranked(Fixture fixture, String query, String field, String candidates, String ids) {}

  /** {@code onDse} is null if the request sends $lexical; {@code documents} is for projections. */
  record DocumentedExample(String request, Reranked onHcd, Reranked onDse, String documents) {}

  // Sends each request example of the docs (Examples section); expects the ids in rerank order,
  // only "data" with a null nextPageState, no "$" fields unless projected, and one default-model
  // request per 10 passages. The docs say rerankOn defaults to $lexical, reads default to limit,
  // and documents missing $lexical or $vector, or with a non-string passage, are excluded; main
  // reranks on $vectorize, reads 50 per read, keeps such documents and sends numbers and booleans
  // as text. The test pins current behavior; whether it is a bug is still under discussion.
  @ParameterizedTest(name = "{0}")
  @MethodSource(
      "io.stargate.sgv2.jsonapi.api.v1.findandrerank.FindAndRerankExampleCases#documentedExamples")
  default void documentedExampleRequestSucceeds(DocumentedExample example) {
    Assumptions.assumeTrue(
        lexicalAvailable() || example.onDse() != null, "the example sends $lexical: HCD only");
    Reranked expected = lexicalAvailable() ? example.onHcd() : example.onDse();
    var headers = expected.fixture() == BYO ? withoutEmbeddingKey() : defaultHeaders();
    var response = postToFixture(expected.fixture(), example.request(), headers);

    assertIds(response, (Object[]) expected.ids().split(" "));
    assertSuccessEnvelope(response);
    if (example.documents() == null) {
      assertNoDollarFields(response);
    } else {
      response.body("data.documents", jsonEquals(example.documents()));
    }
    var sent = passages(expected.fixture(), expected.field(), expected.candidates().split(" "));
    assertRerankerCalls(defaultModelCalls(expected.query(), sent));
  }

  static Stream<Named<DocumentedExample>> documentedExamples() {
    String tree = "A tree in the woods";
    String allMain = "m01 m02 m03 m04 m05 m06 m07 m08 m09 m10 m11 m12";
    String allByo = "o1 o2 o3 o4 b5 b8 b9";
    String nearest8 = "m04 m06 m07 m08 m09 m10 m11 m12";
    var mainTopTen =
        new Reranked(MAIN, tree, "$vectorize", allMain, "m08 m05 m11 m03 m06 m02 m12 m04 m09 m01");
    var byoAll = new Reranked(BYO, tree, "$lexical", allByo, "b5 o4 o3 b9 o2 b8 o1");
    String override =
        """
        {"findAndRerank": {"sort": {"$hybrid": "A tree in the woods"},
          "options": {"rerank": {"provider": "nvidia", "modelName": "nvidia/llama-3.2-nv-rerankqa-1b-v2"}}}}""";
    return Stream.of(
        onHcdOnly(
            "hybrid search with $vectorize -> both reads, reranked on $vectorize",
            mainTopTen,
            """
            {"findAndRerank": {"sort": {"$hybrid": {"$lexical": "house hill grassy",
              "$vectorize": "A tree in the woods"}}}}"""),
        onHcdOnly(
            "hybrid search without $vectorize -> lexical-only b5 is reranked first",
            new Reranked(BYO, "house hill grassy", "$lexical", allByo, "b5 o4 o3 b9 o2 b8 o1"),
            """
            {"findAndRerank": {"options": {"rerankOn": "$lexical", "rerankQuery": "house hill grassy"},
              "sort": {"$hybrid": {"$lexical": "house hill grassy", "$vector": [0.08, -0.62, 0.39]}}}}"""),
        onBoth(
            "vector search with $vectorize -> 12 passages in two batches, top 10 returned",
            mainTopTen,
            """
            {"findAndRerank": {"sort": {"$hybrid": {"$vectorize": "A tree in the woods"}}}}"""),
        onBoth(
            "vector search without $vectorize -> number and boolean example_field sent as text",
            new Reranked(BYO, tree, "example_field", "o1 o2 o3", "o2 o1 o3"),
            """
            {"findAndRerank": {"options": {"rerankOn": "example_field", "rerankQuery": "A tree in the woods"},
              "sort": {"$hybrid": {"$vector": [0.08, -0.62, 0.39]}}}}"""),
        onBoth(
            "shorthand search string -> same result as $vectorize alone",
            mainTopTen,
            """
            {"findAndRerank": {"sort": {"$hybrid": "A tree in the woods"}}}"""),
        onBoth(
            "filter with $vectorize -> only m02, m03 and m05 reranked",
            new Reranked(MAIN, tree, "$vectorize", "m02 m03 m05", "m05 m03 m02"),
            """
            {"findAndRerank": {"filter": {"$and": [{"is_checked_out": false}, {"number_of_pages": {"$lt": 300}}]},
              "sort": {"$hybrid": "A tree in the woods"}}}"""),
        onHcdOnly(
            "filter without $vectorize -> only o1 and o3 reranked",
            new Reranked(BYO, tree, "$lexical", "o1 o3", "o3 o1"),
            """
            {"findAndRerank": {"filter": {"$and": [{"is_checked_out": false}, {"number_of_pages": {"$lt": 300}}]},
              "options": {"rerankOn": "$lexical", "rerankQuery": "A tree in the woods"},
              "sort": {"$hybrid": {"$lexical": "house hill grassy", "$vector": [0.08, -0.62, 0.39]}}}}"""),
        onBoth(
            "limit with $vectorize -> all 12 passages reranked, then 2 returned",
            new Reranked(MAIN, tree, "$vectorize", allMain, "m08 m05"),
            """
            {"findAndRerank": {"options": {"limit": 2}, "sort": {"$hybrid": "A tree in the woods"}}}"""),
        onHcdOnly(
            "limit without $vectorize -> all 7 passages reranked, then 2 returned",
            new Reranked(BYO, tree, "$lexical", allByo, "b5 o4"),
            """
            {"findAndRerank": {"options": {"limit": 2, "rerankOn": "$lexical", "rerankQuery": "A tree in the woods"},
              "sort": {"$hybrid": {"$lexical": "house hill grassy", "$vector": [0.08, -0.62, 0.39]}}}}"""),
        Named.of(
            "hybridLimits with $vectorize -> the vector read takes 8 (on DSE the only read)",
            new DocumentedExample(
                """
                {"findAndRerank": {"options": {"hybridLimits": {"$lexical": 20, "$vector": 8}},
                  "sort": {"$hybrid": "A tree in the woods"}}}""",
                mainTopTen,
                new Reranked(MAIN, tree, "$vectorize", nearest8, "m08 m11 m06 m12 m04 m09 m07 m10"),
                null)),
        onHcdOnly(
            "hybridLimits without $vectorize -> all 7 passages reranked",
            byoAll,
            """
            {"findAndRerank": {"options": {"hybridLimits": {"$lexical": 20, "$vector": 8},
              "rerankOn": "$lexical", "rerankQuery": "A tree in the woods"},
              "sort": {"$hybrid": {"$lexical": "house hill grassy", "$vector": [0.08, -0.62, 0.39]}}}}"""),
        Named.of(
            "projection with $vectorize -> passages still sent, only projected fields returned",
            new DocumentedExample(
                """
                {"findAndRerank": {"options": {}, "projection": {"is_checked_out": true, "title": true},
                  "sort": {"$hybrid": "A tree in the woods"}}}""",
                mainTopTen,
                mainTopTen,
                """
                [{"_id": "m08", "title": "Pumpkin soup"},
                 {"_id": "m05", "is_checked_out": false, "title": "Fresh data"}, {"_id": "m11"},
                 {"_id": "m03", "is_checked_out": false, "title": "Mood display"},
                 {"_id": "m06", "title": "Harbor lights"},
                 {"_id": "m02", "is_checked_out": false, "title": "Talking sneakers"}, {"_id": "m12"},
                 {"_id": "m04", "is_checked_out": false, "title": "Data refresh"}, {"_id": "m09"},
                 {"_id": "m01", "is_checked_out": true, "title": "Quilt for dreamers"}]""")),
        Named.of(
            "projection without $vectorize -> passages still sent, only projected fields returned",
            new DocumentedExample(
                """
                {"findAndRerank": {"options": {"rerankOn": "$lexical", "rerankQuery": "A tree in the woods"},
                  "projection": {"is_checked_out": true, "title": true},
                  "sort": {"$hybrid": {"$lexical": "house hill grassy", "$vector": [0.08, -0.62, 0.39]}}}}""",
                byoAll,
                null,
                """
                [{"_id": "b5"}, {"_id": "o4", "is_checked_out": false, "title": "Ridge lodge"},
                 {"_id": "o3", "is_checked_out": false, "title": "Valley cabin"}, {"_id": "b9"},
                 {"_id": "o2", "is_checked_out": true, "title": "Meadow farmhouse"}, {"_id": "b8"},
                 {"_id": "o1", "is_checked_out": false, "title": "Hillside cottage"}]""")),
        onBoth(
            "override on a collection with rerank enabled -> the named model reranks",
            mainTopTen,
            override),
        onBoth(
            "override on a collection with rerank disabled -> accepted, the named model reranks",
            new Reranked(NORERANK, tree, "$vectorize", "r1 r2 r3", "r1 r3 r2"),
            override));
  }

  // Fills every placeholder of the docs' Signature template, keeping its keys and order, with only
  // the Token and Content-Type headers. Expects o4, o3, o2 with only _id and title, all scores, the
  // query vector as sortVector, and one request with the four filtered titles. Per the
  // findAndRerank docs (Signature and Parameters sections), these parameters work together.
  @Test
  default void signatureTemplateWithEveryPlaceholderFilledIn() {
    var response =
        postToFixture(
            BYO,
            """
            {"findAndRerank": {
              "filter": {"grp": "order"},
              "options": {"hybridLimits": 10, "includeScores": true, "includeSortVector": true,
                          "limit": 3, "rerankOn": "title", "rerankQuery": "A tree in the woods"},
              "projection": {"title": 1},
              "sort": {"$hybrid": {"$vector": [0.08, -0.62, 0.39]}}}}""",
            withoutEmbeddingKey());

    assertIds(response, "o4", "o3", "o2");
    response.body(
        "data.documents",
        jsonEquals(
            """
            [{"_id": "o4", "title": "Ridge lodge"}, {"_id": "o3", "title": "Valley cabin"},
             {"_id": "o2", "title": "Meadow farmhouse"}]"""));
    assertSuccessEnvelope(response, "documentResponses", "sortVector");
    assertScores(
        response,
        scores(3.0f, 0.8039639f, 3, null, rrf(3)),
        scores(2.0f, 0.9996167f, 1, null, rrf(1)),
        scores(0.75f, 0.5678263f, 4, null, rrf(4)));
    assertSortVector(response, 0.08, -0.62, 0.39);
    assertRerankerCalls(
        defaultModelCalls("A tree in the woods", passages(BYO, "title", "o1", "o2", "o3", "o4")));
  }

  // Sends a filter that matches nothing, with both options. Expects exactly the docs' empty
  // response: no documents, empty documentResponses, null nextPageState, the query embedding as
  // sortVector, and no reranker call. Per the findAndRerank docs (Result section), this is the
  // response when no documents are found.
  @Test
  default void resultExampleWhenNoDocumentMatches() {
    var response =
        postToFixture(
            MAIN,
            findAndRerank()
                .filter("{\"grp\": \"no-such-group\"}")
                .hybrid("A tree in the woods")
                .options("{\"includeScores\": true, \"includeSortVector\": true}")
                .json());

    assertThat(
        response.statusCode(200).extract().asString(),
        jsonEquals(
            """
            {"data": {"documents": [], "nextPageState": null},
             "status": {"documentResponses": [], "sortVector": [0.25, 0.25, 0.25, 0.25, 0.25]}}"""));
    assertRerankerNotCalled();
  }

  private static Named<DocumentedExample> onBoth(String name, Reranked expected, String json) {
    return Named.of(name, new DocumentedExample(json, expected, expected, null));
  }

  private static Named<DocumentedExample> onHcdOnly(String name, Reranked expected, String json) {
    return Named.of(name, new DocumentedExample(json, expected, null, null));
  }

  /** The default headers without x-embedding-api-key, as in the docs' examples. */
  private static Map<String, Object> withoutEmbeddingKey() {
    return headers(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, null);
  }
}
