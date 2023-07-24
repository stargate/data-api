package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class VectorSearchIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  private static final String collectionName = "my_collection";

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void happyPathVectorSearch() {
      String json =
          """
        {
          "createCollection": {
            "name" : "my_collection",
            "options": {
              "vector": {
                "size": 4,
                "function": "cosine"
              }
            }
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class InsertOneCollection {
    @Test
    public void inserVectorSearch() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "1",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you",
                  "$vector": [0.1, 0.15, 0.3, 0.12, 0.05]
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
          .body("status.insertedIds[0]", is("1"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
        {
          "find": {
            "filter" : {"_id" : "1"}
          }
        }
        """;
      String expected =
          """
        {
          "_id": "1",
          "name": "Coded Cleats",
          "description": "ChatGPT integrated sneakers that talk to you",
          "$vector": [0.1, 0.15, 0.3, 0.12, 0.05]
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inserVectorCollectionWithoutVectorData() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "10",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you"
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
          .body("status.insertedIds[0]", is("10"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
        {
          "find": {
            "filter" : {"_id" : "10"}
          }
        }
        """;
      String expected =
          """
        {
          "_id": "10",
          "name": "Coded Cleats",
          "description": "ChatGPT integrated sneakers that talk to you"
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inserEmptyVectorData() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "Invalid",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you",
                  "$vector": []
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
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vector field can't be empty"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void inserInvalidVectorData() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "Invalid",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you",
                  "$vector": [0.11, "abc", true, null]
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
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vector search needs to be array of numbers"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(3)
  class InsertManyCollection {
    @Test
    public void inserVectorSearch() {
      String json =
          """
        {
           "insertMany": {
              "documents": [
                {
                  "_id": "2",
                  "name": "Logic Layers",
                  "description": "An AI quilt to help you sleep forever",
                  "$vector": [0.45, 0.09, 0.01, 0.2, 0.11]
                },
                {
                  "_id": "3",
                  "name": "Vision Vector Frame",
                  "description": "Vision Vector Frame', 'A deep learning display that controls your mood",
                  "$vector": [0.1, 0.05, 0.08, 0.3, 0.6]
                }
              ]
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
          .body("status.insertedIds[0]", is("2"))
          .body("status.insertedIds[1]", is("3"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
        {
          "find": {
            "filter" : {"_id" : "2"}
          }
        }
        """;
      String expected =
          """
        {
            "_id": "2",
            "name": "Logic Layers",
            "description": "An AI quilt to help you sleep forever",
            "$vector": [0.45, 0.09, 0.01, 0.2, 0.11]
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }
  }
}
