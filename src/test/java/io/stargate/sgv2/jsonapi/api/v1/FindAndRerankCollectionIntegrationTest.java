package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsDDLSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand} running against a
 * collection.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
public class FindAndRerankCollectionIntegrationTest extends AbstractCollectionIntegrationTestBase {

  // used to cleanup the collection from a previous test, if non null
  private String cleanupCollectionName = null;

  @BeforeAll
  public final void createDefaultCollection() {
    // override, we do not want the basic collection from the base class.
  }

  @AfterEach
  public void cleanup() {
    if (cleanupCollectionName != null) {
      var json =
              """
          {"dropCollection": {"name": "%s"}}
          """
              .formatted(cleanupCollectionName);
      given()
          .port(getTestPort())
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess());
    }
  }

  public void errorOnNotEnabled(
      String collectionName, String collectionSpec, String errorCode, String errorMessageContains) {

    var createCollection = collectionSpec.formatted(collectionName);
    createComplexCollection(createCollection);

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
  public void voidFailOnVectorizeDisabled() {
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
  public void voidFailOnLexicalDisabled() {
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
        "Lexical search is not enabled for collection");
  }
}
