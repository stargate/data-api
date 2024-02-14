package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(
    value =
        io.stargate.sgv2.jsonapi.api.v1.EstimatedDocumentCountIntegrationTest
            .EstimatedDocumentCountTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class EstimatedDocumentCountIntegrationTest extends AbstractCollectionIntegrationTestBase {

  public static final int MAX_ITERATIONS = 1000;

  // Need to set max count limit to -1, and count page size to -1 to avoid pagination
  public static class EstimatedDocumentCountTestResource extends DseTestResource {
    public EstimatedDocumentCountTestResource() {}

    @Override
    public int getMaxCountLimit() {
      return -1;
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Count {

    private void insertMany() {

      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                    "username": "user1",
                    "subdoc" : {
                       "id" : "12345"
                    },
                    "array" : [
                        "value1"
                    ]
                },
                {
                    "username": "user2",
                    "subdoc" : {
                       "id" : "abc"
                    },
                    "array" : [
                        "value2"
                    ]
                },
                {
                    "username": "user3",
                    "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
                    "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
                },
                {
                    "username": "user4",
                    "indexedObject" : { "0": "value_0", "1": "value_1" }
                },
                {
                    "username": "user5",
                    "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                }
              ],
              "options" : {
                "ordered" : true
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(1)
    public void insertDocuments() throws InterruptedException {
      String jsonEstimatedCount =
          """
          {
            "estimatedDocumentCount": {
            }
          }
          """;

      String jsonActualCount =
          """
          {
             "countDocuments": {
             }
          }
          """;

      // execute the insertMany command in a loop until the response indicates a non-zero count, or
      // we have executed the command 100 times
      int tries = 1;
      while (tries <= MAX_ITERATIONS) {
        insertMany();

        int estimatedCount =
            given()
                .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                .contentType(ContentType.JSON)
                .body(jsonEstimatedCount)
                .when()
                .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                .then()
                .statusCode(200)
                .body("errors", is(nullValue()))
                .extract()
                .response()
                .jsonPath()
                .getInt("status.count");

        int actualCount =
            given()
                .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                .contentType(ContentType.JSON)
                .body(jsonActualCount)
                .when()
                .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                .then()
                .statusCode(200)
                .body("errors", is(nullValue()))
                .extract()
                .response()
                .jsonPath()
                .getInt("status.count");

        System.out.println(
            "Iteration: "
                + tries
                + ", Docs inserted: "
                + tries * 5
                + ", Actual count: "
                + actualCount
                + ", Estimated count: "
                + estimatedCount);

        if (estimatedCount > 0) {
          break;
        }
        Thread.sleep(1000);
        tries++;
      }
    }

    /**
     * Verify that truncating the collection (DeleteMany with no filter) yields a zero estimated
     * document count.
     */
    @Test
    @Order(2)
    public void truncate() {

      String json =
          """
              {
                "deleteMany": {
                }
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(-1))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure estimated doc count is zero
      // does not find the documents
      json = """
      {
        "estimatedDocumentCount": {
      }
      }
      """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(0))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      EstimatedDocumentCountIntegrationTest.super.checkMetrics("EstimatedDocumentCountCommand");
    }
  }
}
