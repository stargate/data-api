package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.RequestException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand} running against a
 * collection.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class FindAndRerankCollectionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  // used to cleanup the collection from a previous test, if non-null
  private String cleanupCollectionName = null;

  @BeforeAll
  public final void createDefaultCollection() {
    // override, we do not want the basic collection from the base class.
  }

  @AfterEach
  public void cleanup() {
    if (cleanupCollectionName != null) {
      String toDelete = cleanupCollectionName;
      cleanupCollectionName = null; // clear out just in case
      deleteCollection(toDelete);
    }
  }

  @Test
  public void failOnVectorDisabled() {
    errorOnNotEnabled(
        "vector_not_enabled",
        """
        {
          "name" : "%s",
          "options": {}
        }
        """,
        "UNSUPPORTED_VECTOR_SORT_FOR_COLLECTION",
        "The Collection %s.%s does not have vectors enabled.");
  }

  @Test
  void failOnVectorizeDisabled() {
    errorOnNotEnabled(
        "vectorize_not_enabled",
        """
        {
          "name" : "%s",
          "options": {
            "vector": {
                  "metric": "cosine",
                  "dimension": 1024
            }
          }
        }
        """,
        "UNSUPPORTED_VECTORIZE_SORT_FOR_COLLECTION",
        "The Collection %s.%s does not have vectorize enabled.");
  }

  @Test
  void failOnLexicalDisabled() {
    errorOnNotEnabled(
        "lexical_not_enabled",
        """
        {
          "name" : "%s",
          "options": {
            "vector": {
                    "metric": "cosine",
                    "dimension": 1024,
                    "service": {
                        "provider": "openai",
                        "modelName": "text-embedding-3-small"
                    }
                },
            "lexical": {
                "enabled": false
            }
          }
        }
        """,
        "LEXICAL_NOT_ENABLED_FOR_COLLECTION",
        "only be used on Collections for which Lexical feature is enabled");
  }

  // https://github.com/stargate/data-api/issues/2057
  @Test
  void failOnEmptyRequest() {
    // Must not fail for "no lexical available", so skip on DSE
    Assumptions.assumeTrue(isLexicalAvailableForDB());

    String collectionName = "find_rerank_empty_request";
    createCollectionWithCleanup(
        collectionName,
        """
            {
              "name" : "%s",
              "options": {
                "vector": {
                        "metric": "cosine",
                        "dimension": 1024,
                        "service": {
                            "provider": "openai",
                            "modelName": "text-embedding-3-small"
                        }
                    },
                "lexical": {
                  "enabled": true,
                  "analyzer": "standard"
                }
              }
            }
            """);

    givenHeadersPostJsonThen(keyspaceName, collectionName, "{\"findAndRerank\": { } }")
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(RequestException.Code.MISSING_RERANK_QUERY_TEXT.name()))
        .body(
            "errors[0].message",
            containsString(
                "findAndRerank command is missing the text to use as the query with the reranking"));
  }

  private void errorOnNotEnabled(
      String collectionName, String collectionSpec, String errorCode, String errorMessageContains) {
    createCollectionWithCleanup(collectionName, collectionSpec);

    var rerank =
        """
          {"findAndRerank": {
                  "filter": {},
                  "projection": {},
                  "sort": {
                      "$hybrid": "hybrid sort"
                  },
                  "options": {
                      "limit" : 10,
                      "hybridLimits" : 10,
                      "includeScores": true,
                      "includeSortVector": false
                  }
              }
          }
          """;

    givenHeadersPostJsonThen(keyspaceName, collectionName, rerank)
        .body("$", responseIsError())
        .body("errors[0].errorCode", is(errorCode))
        .body(
            "errors[0].message",
            containsString(errorMessageContains.formatted(keyspaceName, collectionName)));
  }

  private void createCollectionWithCleanup(String collectionName, String collectionSpec) {
    createComplexCollection(collectionSpec.formatted(collectionName));
    // save the collection name for cleanup, but only after successful creation
    cleanupCollectionName = collectionName;
  }
}
