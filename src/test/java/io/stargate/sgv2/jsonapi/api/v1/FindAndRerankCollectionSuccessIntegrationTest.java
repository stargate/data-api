package io.stargate.sgv2.jsonapi.api.v1;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiResponseValidator;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
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

/** Successful collection reranking through the production Nvidia HTTP client and task pipeline. */
@QuarkusIntegrationTest
@WithTestResource(DseTestResource.class)
@QuarkusTestResource(value = RerankingTestResource.class, restrictToAnnotatedClass = true)
public class FindAndRerankCollectionSuccessIntegrationTest
    extends AbstractCollectionIntegrationTestBase {

  private static final List<Double> VECTOR = List.of(0.25, 0.25, 0.25, 0.25, 0.25);
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
  void nonStringPassagesAreExcluded() {
    insertDocuments(
        collectionName,
        List.of(
            Map.of("_id", "text", "$vector", VECTOR, "content", "score:1 text passage"),
            Map.of("_id", "number", "$vector", VECTOR, "content", 42),
            Map.of("_id", "boolean", "$vector", VECTOR, "content", true),
            Map.of("_id", "array", "$vector", VECTOR, "content", List.of("score:2 array")),
            Map.of("_id", "object", "$vector", VECTOR, "content", Map.of("text", "score:3"))));

    findAndRerank(
            collectionName,
            Map.of(
                "sort", Map.of("$hybrid", Map.of("$vector", VECTOR)),
                "options", Map.of("rerankQuery", "query", "rerankOn", "content")))
        .body("data.documents._id", contains("text"));

    assertThat(requestedPassages()).containsExactly("score:1 text passage");
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

  private void insertDocuments(String collection, List<? extends Map<String, Object>> documents) {
    new DataApiResponseValidator(
            CommandName.INSERT_MANY,
            givenHeadersPostJsonThenOk(
                keyspaceName,
                collection,
                Json.write(Map.of("insertMany", Map.of("documents", documents)))))
        .wasSuccessful()
        .hasInsertedIdCount(documents.size());
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
