package io.stargate.sgv2.jsonapi.api.v1;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.Lists;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.testresource.RerankingTestResource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Successful collection reranking through the production Nvidia HTTP client and task pipeline. */
@QuarkusIntegrationTest
@WithTestResource(DseTestResource.class)
@QuarkusTestResource(value = RerankingTestResource.class, restrictToAnnotatedClass = true)
public class FindAndRerankCollectionSuccessIntegrationTest
    extends AbstractCollectionIntegrationTestBase {

  private static final List<Double> VECTOR = List.of(0.25, 0.25, 0.25, 0.25, 0.25);

  /** The most documents insertDocuments() sends in one insertMany. */
  private static final int INSERT_BATCH_SIZE = 20;

  private WireMockServer reranker;

  @Override
  @BeforeAll
  public void createDefaultCollection() {
    createRerankingCollection(collectionName, true, false);
  }

  @BeforeEach
  void resetDocumentsAndRequests() {
    deleteAllDocuments();
    reranker.resetRequests();
  }

  @Override
  protected Map<String, ?> getHeaders() {
    var headers = new HashMap<String, Object>(super.getHeaders());
    headers.put(
        HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME, RerankingTestResource.API_KEY);
    return headers;
  }

  @Test
  void vectorizeSuppliesDefaultQueryAndPassages() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "low", "$vectorize", "score:1 low passage"),
            Map.of("_id", "high", "$vectorize", "score:5 high passage"),
            Map.of("_id", "middle", "$vectorize", "score:3 middle passage")));

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$hybrid", Map.of("$vectorize", "default search query")),
                "options", Map.of("includeScores", true, "includeSortVector", true)))
        .body("data.documents._id", contains("high", "middle", "low"))
        .body("status.documentResponses.scores.'$rerank'", contains(5.0f, 3.0f, 1.0f))
        .body("status.sortVector", contains(0.25f, 0.25f, 0.25f, 0.25f, 0.25f))
        .hasNoField("data.nextPageState");

    assertThat(requestedPassages())
        .containsExactlyInAnyOrder(
            "score:1 low passage", "score:5 high passage", "score:3 middle passage");
    assertRequestsUseQuery("default search query");
    reranker.verify(1, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  @Test
  void explicitOverridesRerankBeforeProjectionAndApplyFinalLimit() {
    var overrideCollection = collectionName + "_override";
    createRerankingCollection(overrideCollection, false, false);
    try {
      insertDocuments(
          overrideCollection,
          List.of(
              Map.of(
                  "_id",
                  "low",
                  "name",
                  "low",
                  "$vectorize",
                  "score:90 ignored",
                  "article",
                  Map.of("text", "score:1 low")),
              Map.of(
                  "_id",
                  "high",
                  "name",
                  "high",
                  "$vectorize",
                  "score:10 ignored",
                  "article",
                  Map.of("text", "score:8 high")),
              Map.of(
                  "_id",
                  "middle",
                  "name",
                  "middle",
                  "$vectorize",
                  "score:30 ignored",
                  "article",
                  Map.of("text", "score:4 middle"))));

      findAndRerank(
              overrideCollection,
              Map.of(
                  "sort", Map.of("$hybrid", Map.of("$vectorize", "embedding query")),
                  "projection", Map.of("name", 1, "_id", 0),
                  "options",
                      Map.of(
                          "rerankQuery",
                          "explicit reranking query",
                          "rerankOn",
                          "article.text",
                          "rerank",
                          Map.of(
                              "provider",
                              "nvidia",
                              "modelName",
                              "nvidia/llama-3.2-nv-rerankqa-1b-v2"),
                          "limit",
                          2,
                          "includeScores",
                          true)))
          .body("data.documents", contains(Map.of("name", "high"), Map.of("name", "middle")))
          .body("status.documentResponses.scores.'$rerank'", contains(8.0f, 4.0f))
          .hasNoField("status.sortVector")
          .hasNoField("data.nextPageState");

      assertThat(requestedPassages())
          .containsExactlyInAnyOrder("score:1 low", "score:8 high", "score:4 middle");
      assertRequestsUseQuery("explicit reranking query");
    } finally {
      deleteCollection(overrideCollection);
    }
  }

  @Test
  void scoresAndSortVectorAreOptional() {
    insertDocuments(
        collectionName, List.of(Map.of("_id", "one", "$vectorize", "score:2 single passage")));

    findAndRerank(
            collectionName,
            Map.of("sort", Map.of("$hybrid", Map.of("$vectorize", "default query"))))
        .body("data.documents._id", contains("one"))
        .hasNoField("status.documentResponses")
        .hasNoField("status.sortVector");
    reranker.verify(1, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  @Test
  void noCandidatesAvoidProviderCalls() {
    findAndRerank(collectionName, Map.of("sort", Map.of("$hybrid", Map.of("$vectorize", "query"))))
        .body("data.documents", empty());
    reranker.verify(0, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  @Test
  void missingPassagesAvoidProviderCalls() {
    insertDocuments(collectionName, List.of(Map.of("_id", "missing", "$vector", VECTOR)));

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$hybrid", Map.of("$vector", VECTOR)),
                "options", Map.of("rerankQuery", "query", "rerankOn", "missingPassage")))
        .body("data.documents", empty());
    reranker.verify(0, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  void hybridCandidatesAreDeduplicatedBeforeReranking() {
    var hybridCollection = collectionName + "_hybrid";
    createRerankingCollection(hybridCollection, true, true);
    try {
      insertDocuments(
          hybridCollection,
          IntStream.rangeClosed(1, 3)
              .mapToObj(
                  index ->
                      Map.<String, Object>of(
                          "_id", "doc" + index,
                          "$vector", VECTOR,
                          "$lexical", "shared lexical passage " + index,
                          "content", "score:" + index + " shared passage " + index))
              .toList());

      findAndRerank(
              hybridCollection,
              Map.of(
                  "sort", Map.of("$hybrid", Map.of("$vector", VECTOR, "$lexical", "shared")),
                  "options",
                      Map.of("rerankQuery", "query", "rerankOn", "content", "includeScores", true)))
          .body("data.documents._id", contains("doc3", "doc2", "doc1"))
          .body("status.documentResponses.scores.'$rerank'", contains(3.0f, 2.0f, 1.0f))
          .body("status.documentResponses.scores.'$vectorRank'", everyItem(notNullValue()))
          .body("status.documentResponses.scores.'$bm25Rank'", everyItem(notNullValue()));

      assertThat(requestedPassages())
          .containsExactlyInAnyOrder(
              "score:1 shared passage 1", "score:2 shared passage 2", "score:3 shared passage 3");
      reranker.verify(1, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
    } finally {
      deleteCollection(hybridCollection);
    }
  }

  @Test
  void allSixtyCandidatesReachRerankerAcrossBatches() {
    var documents =
        IntStream.range(0, 60)
            .mapToObj(
                index ->
                    Map.<String, Object>of(
                        "_id", "doc" + index,
                        "$vector", List.of((double) index + 1, 0.0, 0.0, 0.0, 0.0),
                        "content", "score:" + index + " unique passage " + index))
            .toList();
    insertDocuments(collectionName, documents);
    var queryVector = List.of(1.0, 0.0, 0.0, 0.0, 0.0);

    // The best reranking candidate is beyond the first 50 documents in vector order.
    new DataApiResponseValidator(
            CommandName.FIND,
            givenHeadersPostJsonThenOk(
                Json.write(
                    Map.of(
                        "find",
                        Map.of(
                            "sort",
                            Map.of("$vector", queryVector),
                            "options",
                            Map.of("limit", 50))))))
        .wasSuccessful()
        .body("data.documents", hasSize(50))
        .body("data.documents._id", not(hasItem("doc59")));

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$hybrid", Map.of("$vector", queryVector)),
                "options",
                    Map.of(
                        "hybridLimits",
                        60,
                        "rerankQuery",
                        "query",
                        "rerankOn",
                        "content",
                        "limit",
                        1,
                        "includeScores",
                        true)))
        .body("data.documents._id", contains("doc59"))
        .body("status.documentResponses.scores.'$rerank'", contains(59.0f));

    assertThat(requestedPassages())
        .containsExactlyInAnyOrderElementsOf(
            documents.stream().map(doc -> (String) doc.get("content")).toList());
    reranker.verify(6, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
    assertThat(requests())
        .allSatisfy(
            request ->
                assertThat(Json.node(request.getBodyAsString()).get("passages").size())
                    .isEqualTo(10));
  }

  @Test
  void vectorizeSortWithoutHybridSuppliesDefaultQueryAndPassages() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "low", "$vectorize", "score:1 low passage"),
            Map.of("_id", "high", "$vectorize", "score:5 high passage"),
            Map.of("_id", "middle", "$vectorize", "score:3 middle passage")));

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$vectorize", "default search query"),
                "options", Map.of("includeScores", true, "includeSortVector", true)))
        .body("data.documents._id", contains("high", "middle", "low"))
        .body("status.documentResponses.scores.'$rerank'", contains(5.0f, 3.0f, 1.0f))
        .body("status.documentResponses.scores.'$vectorRank'", everyItem(notNullValue()))
        .body("status.documentResponses.scores.'$bm25Rank'", everyItem(nullValue()))
        .body("status.sortVector", contains(0.25f, 0.25f, 0.25f, 0.25f, 0.25f))
        .hasNoField("data.nextPageState");

    assertThat(requestedPassages())
        .containsExactlyInAnyOrder(
            "score:1 low passage", "score:5 high passage", "score:3 middle passage");
    assertRequestsUseQuery("default search query");
    reranker.verify(1, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  /** The $vector sort is an array of numbers, or the same vector as a $binary vector. */
  @ParameterizedTest(name = "binary={0}")
  @ValueSource(booleans = {false, true})
  void vectorSortWithoutHybridUsesRerankOptions(boolean binary) {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "low", "$vector", VECTOR, "content", "score:1 low passage"),
            Map.of("_id", "high", "$vector", VECTOR, "content", "score:5 high passage")));
    Object vector =
        binary
            ? Map.of(
                "$binary",
                generateBase64EncodedBinaryVector(new float[] {0.25f, 0.25f, 0.25f, 0.25f, 0.25f}))
            : VECTOR;

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$vector", vector),
                "options",
                    Map.of(
                        "rerankQuery",
                        "explicit reranking query",
                        "rerankOn",
                        "content",
                        "includeSortVector",
                        true)))
        .body("data.documents._id", contains("high", "low"))
        .body("status.sortVector", contains(0.25f, 0.25f, 0.25f, 0.25f, 0.25f));

    assertThat(requestedPassages())
        .containsExactlyInAnyOrder("score:1 low passage", "score:5 high passage");
    assertRequestsUseQuery("explicit reranking query");
  }

  @Test
  @DisabledIfSystemProperty(named = TEST_PROP_LEXICAL_DISABLED, matches = "true")
  void lexicalSortWithoutHybridSuppliesDefaultQueryAndPassages() {
    var lexicalCollection = collectionName + "_lexical";
    createRerankingCollection(lexicalCollection, true, true);
    try {
      insertDocuments(
          lexicalCollection,
          List.of(
              Map.of("_id", "low", "$lexical", "score:1 cheese low"),
              Map.of("_id", "high", "$lexical", "score:5 cheese high"),
              Map.of("_id", "middle", "$lexical", "score:3 cheese middle"),
              // the lexical read does not find it, so it is not reranked
              Map.of("_id", "other", "$lexical", "score:9 butter")));

      findAndRerank(
              lexicalCollection,
              Map.of(
                  "sort", Map.of("$lexical", "cheese"),
                  "options", Map.of("includeScores", true, "includeSortVector", true)))
          .body("data.documents._id", contains("high", "middle", "low"))
          .body("status.documentResponses.scores.'$rerank'", contains(5.0f, 3.0f, 1.0f))
          .body("status.documentResponses.scores.'$vectorRank'", everyItem(nullValue()))
          .body("status.documentResponses.scores.'$bm25Rank'", everyItem(notNullValue()))
          .body("status.sortVector", nullValue());

      assertThat(requestedPassages())
          .containsExactlyInAnyOrder(
              "score:1 cheese low", "score:5 cheese high", "score:3 cheese middle");
      assertRequestsUseQuery("cheese");
    } finally {
      deleteCollection(lexicalCollection);
    }
  }

  /**
   * A sort without $hybrid reads the default of 50 documents, or rerankCandidateLimit documents.
   * The best reranking candidate is beyond the first 50 documents in vector order.
   */
  @Test
  void sortWithoutHybridReadsRerankCandidateLimitDocuments() {
    var documents =
        IntStream.range(0, 60)
            .mapToObj(
                index ->
                    Map.<String, Object>of(
                        "_id", "candidate" + index,
                        "$vector", List.of((double) index + 1, 0.0, 0.0, 0.0, 0.0),
                        "content", "score:" + index + " unique passage " + index))
            .toList();
    insertDocuments(collectionName, documents);
    var sort = Map.of("$vector", List.of(1.0, 0.0, 0.0, 0.0, 0.0));

    findAndRerank(
            collectionName,
            Map.of(
                "sort",
                sort,
                "options",
                Map.of("rerankQuery", "query", "rerankOn", "content", "limit", 1)))
        .body("data.documents._id", contains("candidate49"));
    assertThat(requestedPassages())
        .containsExactlyInAnyOrderElementsOf(
            documents.subList(0, 50).stream().map(doc -> (String) doc.get("content")).toList());

    reranker.resetRequests();
    findAndRerank(
            collectionName,
            Map.of(
                "sort",
                sort,
                "options",
                Map.of(
                    "rerankCandidateLimit",
                    60,
                    "rerankQuery",
                    "query",
                    "rerankOn",
                    "content",
                    "limit",
                    1)))
        .body("data.documents._id", contains("candidate59"));
    assertThat(requestedPassages())
        .containsExactlyInAnyOrderElementsOf(
            documents.stream().map(doc -> (String) doc.get("content")).toList());
  }

  /**
   * A field sort, an empty sort and no sort read with the filter leg: the documents the filter
   * matches. The filter leg runs on a collection without vector. It uses the rerankQuery and
   * rerankOn options, and the scores have no vector or BM25 rank.
   */
  @Test
  void filterLegReranksDocumentsTheFilterMatches() {
    var filterCollection = collectionName + "_filter";
    createComplexCollection(
        Json.write(
            Map.of(
                "name", filterCollection, "options", Map.of("rerank", Map.of("enabled", true)))));
    try {
      insertDocuments(
          filterCollection,
          List.of(
              Map.of("_id", "nz-20", "country", "nz", "age", 20, "food", "score:3 cheese toast"),
              Map.of("_id", "nz-60", "country", "nz", "age", 60, "food", "score:1 blue cheese"),
              Map.of("_id", "nz-40", "country", "nz", "age", 40, "food", "score:5 cheese board"),
              Map.of("_id", "au-30", "country", "au", "age", 30, "food", "score:9 cheese pie")));
      var nzPassages =
          List.of("score:3 cheese toast", "score:1 blue cheese", "score:5 cheese board");
      var options =
          Map.of("rerankQuery", "who loves cheese?", "rerankOn", "food", "includeScores", true);

      findAndRerank(
              filterCollection,
              Map.of(
                  "filter", Map.of("country", "nz"), "sort", Map.of("age", -1), "options", options))
          .body("data.documents._id", contains("nz-40", "nz-20", "nz-60"))
          .body("status.documentResponses.scores.'$rerank'", contains(5.0f, 3.0f, 1.0f))
          .body("status.documentResponses.scores.'$vectorRank'", everyItem(nullValue()))
          .body("status.documentResponses.scores.'$bm25Rank'", everyItem(nullValue()))
          .hasNoField("data.nextPageState");
      assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(nzPassages);
      assertRequestsUseQuery("who loves cheese?");

      reranker.resetRequests();
      findAndRerank(
              filterCollection,
              Map.of("filter", Map.of("country", "nz"), "sort", Map.of(), "options", options))
          .body("data.documents._id", contains("nz-40", "nz-20", "nz-60"));
      assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(nzPassages);

      reranker.resetRequests();
      findAndRerank(filterCollection, Map.of("filter", Map.of("country", "nz"), "options", options))
          .body("data.documents._id", contains("nz-40", "nz-20", "nz-60"));
      assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(nzPassages);
    } finally {
      deleteCollection(filterCollection);
    }
  }

  /**
   * Documents with the same passage get the same rerank score. The filter leg gives each document
   * an RRF score from the order of the read, so ties are in the order of the field sort. The _ids
   * sort in a different order, so the test fails if ties are in _id order.
   */
  @Test
  void filterLegOrdersEqualRerankScoresByFieldSort() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "a", "age", 20, "food", "score:2 cheese"),
            Map.of("_id", "b", "age", 60, "food", "score:2 cheese"),
            Map.of("_id", "c", "age", 40, "food", "score:2 cheese")));

    findAndRerank(
            collectionName,
            Map.of(
                "sort",
                Map.of("age", -1),
                "options",
                Map.of("rerankQuery", "cheese", "rerankOn", "food", "includeScores", true)))
        .body("data.documents._id", contains("b", "c", "a"))
        .body("status.documentResponses.scores.'$rerank'", everyItem(is(2.0f)));
  }

  /**
   * The filter leg reads the default of 50 documents, or rerankCandidateLimit documents, with or
   * without a field sort. A read of more than the default page size of 50 is not cut to one page.
   */
  @Test
  void filterLegReadsRerankCandidateLimitDocuments() {
    var documents =
        IntStream.range(0, 60)
            .mapToObj(
                index ->
                    Map.<String, Object>of(
                        "_id", "candidate" + index,
                        "rank", index,
                        "content", "score:" + index + " unique passage " + index))
            .toList();
    insertDocuments(collectionName, documents);
    var allPassages = documents.stream().map(doc -> (String) doc.get("content")).toList();
    var sort = Map.of("rank", 1);

    // the field sort reads the 50 documents with the lowest rank by default
    findAndRerank(
            collectionName,
            Map.of(
                "sort",
                sort,
                "options",
                Map.of("rerankQuery", "query", "rerankOn", "content", "limit", 1)))
        .body("data.documents._id", contains("candidate49"));
    assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(allPassages.subList(0, 50));

    var candidateLimitOptions =
        Map.of(
            "rerankCandidateLimit", 60, "rerankQuery", "query", "rerankOn", "content", "limit", 1);

    reranker.resetRequests();
    findAndRerank(collectionName, Map.of("sort", sort, "options", candidateLimitOptions))
        .body("data.documents._id", contains("candidate59"));
    assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(allPassages);

    // without a sort the read order is the database order, so only the count is known by default
    reranker.resetRequests();
    findAndRerank(
            collectionName,
            Map.of("options", Map.of("rerankQuery", "query", "rerankOn", "content", "limit", 1)))
        .body("data.documents", hasSize(1));
    assertThat(requestedPassages()).hasSize(50);

    reranker.resetRequests();
    findAndRerank(collectionName, Map.of("options", candidateLimitOptions))
        .body("data.documents._id", contains("candidate59"));
    assertThat(requestedPassages()).containsExactlyInAnyOrderElementsOf(allPassages);
  }

  /**
   * The filter leg reads rerankCandidateLimit documents, then drops the documents without the
   * rerankOn field. So the command returns fewer documents than its limit, even though the
   * collection has more documents that match.
   */
  @Test
  void filterLegDropsDocumentsWithoutRerankOnAfterTheRead() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "rank0", "rank", 0, "content", "score:1 first passage"),
            Map.of("_id", "rank1", "rank", 1),
            Map.of("_id", "rank2", "rank", 2, "content", "score:3 third passage"),
            Map.of("_id", "rank3", "rank", 3, "content", "score:4 fourth passage")));

    findAndRerank(
            collectionName,
            Map.of(
                "sort",
                Map.of("rank", 1),
                "options",
                Map.of(
                    "rerankCandidateLimit",
                    3,
                    "rerankQuery",
                    "query",
                    "rerankOn",
                    "content",
                    "limit",
                    10)))
        .body("data.documents._id", contains("rank2", "rank0"));
    assertThat(requestedPassages())
        .containsExactlyInAnyOrder("score:1 first passage", "score:3 third passage");
  }

  /** The filter leg reranks on the rerankOn field, even when the projection excludes it. */
  @Test
  void filterLegReranksOnFieldTheProjectionExcludes() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "low", "name", "low", "content", "score:1 low passage"),
            Map.of("_id", "high", "name", "high", "content", "score:5 high passage")));

    findAndRerank(
            collectionName,
            Map.of(
                "projection",
                Map.of("content", 0),
                "options",
                Map.of("rerankQuery", "query", "rerankOn", "content")))
        .body(
            "data.documents",
            contains(Map.of("_id", "high", "name", "high"), Map.of("_id", "low", "name", "low")));
    assertThat(requestedPassages())
        .containsExactlyInAnyOrder("score:1 low passage", "score:5 high passage");
  }

  /**
   * A field sort is an in-memory sort, the same as find. The command fails when the filter matches
   * more documents than the sort can read. The ITs set that limit to 100 documents.
   */
  @Test
  void filterLegFieldSortFailsWhenFilterMatchesTooManyDocuments() {
    insertDocuments(
        collectionName,
        IntStream.range(0, 100)
            .mapToObj(
                index ->
                    Map.<String, Object>of(
                        "_id", "doc" + index, "rank", index, "content", "score:1 passage"))
            .toList());

    new DataApiResponseValidator(
            CommandName.FIND_AND_RERANK,
            givenHeadersPostJsonThenOk(
                keyspaceName,
                collectionName,
                Json.write(
                    Map.of(
                        "findAndRerank",
                        Map.of(
                            "sort",
                            Map.of("rank", 1),
                            "options",
                            Map.of("rerankQuery", "query", "rerankOn", "content"))))))
        .hasSingleApiError(
            SortException.Code.OVERLOADED_SORT_ROW_LIMIT,
            SortException.class,
            "in-memory sorting which has a limit of 100 documents");
    reranker.verify(0, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  private void createRerankingCollection(
      String name, boolean rerankEnabled, boolean lexicalEnabled) {
    var options = new HashMap<String, Object>();
    options.put(
        "vector",
        Map.of(
            "dimension",
            5,
            "metric",
            "euclidean",
            "service",
            Map.of(
                "provider",
                "custom",
                "modelName",
                "text-embedding-ada-002",
                "parameters",
                Map.of("projectId", "test project"))));
    options.put("rerank", Map.of("enabled", rerankEnabled));
    if (lexicalEnabled) {
      options.put("lexical", Map.of("enabled", true));
    }
    createComplexCollection(Json.write(Map.of("name", name, "options", options)));
  }

  /**
   * Inserts the documents in batches of {@link #INSERT_BATCH_SIZE}. An emulated HCD can time out
   * when one insertMany has more documents.
   */
  private void insertDocuments(String collection, List<? extends Map<String, Object>> documents) {
    for (var batch : Lists.partition(documents, INSERT_BATCH_SIZE)) {
      new DataApiResponseValidator(
              CommandName.INSERT_MANY,
              givenHeadersPostJsonThenOk(
                  keyspaceName,
                  collection,
                  Json.write(Map.of("insertMany", Map.of("documents", batch)))))
          .wasSuccessful()
          .hasInsertedIdCount(batch.size());
    }
  }

  private DataApiResponseValidator findAndRerank(String collection, Map<String, Object> command) {
    return new DataApiResponseValidator(
            CommandName.FIND_AND_RERANK,
            givenHeadersPostJsonThenOk(
                keyspaceName, collection, Json.write(Map.of("findAndRerank", command))))
        .wasSuccessful();
  }

  private List<LoggedRequest> requests() {
    return reranker.findAll(postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  private List<String> requestedPassages() {
    var passages = new ArrayList<String>();
    for (var request : requests()) {
      Json.node(request.getBodyAsString())
          .get("passages")
          .forEach(passage -> passages.add(passage.get("text").asText()));
    }
    return passages;
  }

  private void assertRequestsUseQuery(String query) {
    assertThat(requests())
        .isNotEmpty()
        .allSatisfy(
            request -> {
              var body = Json.node(request.getBodyAsString());
              assertThat(body.get("query").get("text").asText()).isEqualTo(query);
              assertThat(body.get("model").asText())
                  .isEqualTo("nvidia/llama-3.2-nv-rerankqa-1b-v2");
              assertThat(body.get("truncate").asText()).isEqualTo("NONE");
            });
  }
}
