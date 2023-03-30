package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
public class FindOneAndReplaceIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  class FindOneAndReplace {
    @Test
    public void byId() {
      String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      String expected =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "status" : false
            }
            """;

      String json =
          """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : { "username": "user3", "status" : false }
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
            {
              "find": {
                "filter" : {"_id" : "doc3"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdWithId() {
      String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      String expected =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "status" : false
            }
            """;

      String json =
          """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : {"_id" : "doc3", "username": "user3", "status" : false }
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
            {
              "find": {
                "filter" : {"_id" : "doc3"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void byIdWithDifferentId() {
      String document =
          """
            {
              "_id": "doc3",
              "username": "user3",
              "active_user" : true
            }
            """;
      insertDoc(document);

      String json =
          """
            {
              "findOneAndReplace": {
                "filter" : {"_id" : "doc3"},
                "replacement" : {"_id" : "doc4", "username": "user3", "status" : false }
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
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].errorCode", is("DOCUMENT_REPLACE_DIFFERENT_DOCID"))
          .body(
              "errors[0].message",
              is("The replace document and document resolved using filter has different _id"));
    }

    @Test
    public void byIdWithEmptyDocument() {
      String document =
          """
                {
                  "_id": "doc3",
                  "username": "user3",
                  "active_user" : true
                }
                """;
      insertDoc(document);

      String expected =
          """
                {
                  "_id": "doc3"
                }
                """;

      String json =
          """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "doc3"},
                    "replacement" : {}
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
          .body("data.docs[0]", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "doc3"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
