package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class InsertIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @AfterEach
  public void cleanUpData() {
    String json = """
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
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        .statusCode(200);
  }

  @Nested
  class InsertOne {

    @Test
    public void insertDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3"
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("doc3"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"doc3",
            "username":"user3"
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void insertDocumentWithNumberId() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": 4,
                "username": "user4"
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is(4))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : 4}
            }
          }
          """;
      String expected =
          """
          {
            "_id": 4,
            "username":"user4"
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void insertDuplicateDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "duplicate",
                "username": "user4"
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("duplicate"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "duplicate",
                "username": "different_user_name"
              }
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", jsonEquals("[]"))
          .body(
              "errors[0].message",
              is(
                  "Failed to insert document with _id 'duplicate': Document already exists with the given _id"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "duplicate"}
            }
          }
          """;
      String expected =
          """
              {
                "_id": "duplicate",
                "username":"user4"
              }
              """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void emptyDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
              }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is(notNullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void notValidDocumentMissing() {
      String json =
          """
          {
            "insertOne": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"));
    }
  }

  @Nested
  class InsertMany {

    @Test
    public void ordered() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", contains("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "countDocuments": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void orderedDuplicateIds() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4_duplicate"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", contains("doc4"))
          .body("data", is(nullValue()))
          .body("errors[0].message", startsWith("Failed to insert document with _id 'doc4'"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
          """
          {
            "countDocuments": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    public void orderedDuplicateDocumentNoNamespace() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .post(CollectionResource.BASE_PATH, "something_else", collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", is(empty()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "Failed to insert document with _id 'doc4': INVALID_ARGUMENT: keyspace something_else does not exist"))
          .body("errors[0].exceptionClass", is("StatusRuntimeException"));
    }

    @Test
    public void unordered() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
                }
              ],
              "options": { "ordered": false }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", containsInAnyOrder("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "countDocuments": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void unorderedDuplicateIds() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
                }
              ],
              "options": { "ordered": false }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", containsInAnyOrder("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors[0].message", startsWith("Failed to insert document with _id 'doc4'"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
          """
          {
            "countDocuments": {
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withDifferentTypes() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "5",
                  "username": "user_id_5"
                },
                {
                  "_id": 5,
                  "username": "user_id_5_number"
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", contains("5", 5))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "5"}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "5",
            "username":"user_id_5"
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : 5}
            }
          }
          """;
      expected =
          """
          {
            "_id": 5,
            "username":"user_id_5_number"
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void emptyDocuments() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {},
                {}
              ]
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", hasSize(2))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }
}
