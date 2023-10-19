package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndReplaceIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byIdWithIdNoChange() {
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
                "replacement" : {"_id" : "doc3", "username": "user3", "active_user" : true }
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(document));
    }

    @Test
    public void withSort() {
      String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      String document1 =
          """
          {
            "_id": "doc2",
            "username": "user2",
            "active_user" : true
          }
          """;
      insertDoc(document1);
      String expected =
          """
              {
                "_id": "doc2",
                "username": "username2",
                "status" : true
              }
              """;

      String json =
          """
              {
                "findOneAndReplace": {
                  "filter" : {"active_user" : true},
                  "sort" : {"username" : 1},
                  "replacement" : {"username": "username2", "status" : true },
                  "options" : {"returnDocument" : "after"}
                }
              }
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "doc2"}
                  }
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void withUpsert() {
      String expected =
          """
        {
          "_id": "doc2",
          "username": "username2",
          "status" : true
        }
        """;

      String json =
          """
        {
          "findOneAndReplace": {
            "filter" : {"_id" : "doc2"},
            "replacement" : {"username": "username2", "status" : true },
            "options" : {"returnDocument" : "after", "upsert" : true}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expected))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is("doc2"))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
        {
          "find": {
            "filter" : {"_id" : "doc2"}
          }
        }
        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void withUpsertNewId() {
      final String newId = "new-id-1234";
      String json =
          """
                {
                  "findOneAndReplace": {
                    "filter" : {},
                    "replacement" : {
                      "username": "aaronm",
                      "_id": "%s"
                    },
                    "options" : {
                      "returnDocument": "after",
                      "upsert": true
                    }
                  }
                }
                """
              .formatted(newId);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("data.document._id", is(newId))
          // Should we return id of new document as upsertedId?
          .body("status.upsertedId", is(newId));

      // assert state after update
      json =
          """
                {
                  "find": {
                    "filter" : {"username" : "aaronm"}
                  }
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]._id", is(newId));
    }

    @Test
    public void withUpsertNoId() {
      String json =
          """
            {
              "findOneAndReplace": {
                "filter" : {"username" : "username2"},
                "replacement" : {"username": "username2", "status" : true },
                "options" : {"returnDocument" : "after", "upsert" : true}
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document._id", is(notNullValue()))
          .body("data.document._id", any(String.class))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is(notNullValue()))
          .body("status.upsertedId", any(String.class))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
            {
              "find": {
                "filter" : {"username" : "username2"}
              }
            }
            """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]._id", is(notNullValue()))
          .body("data.documents[0]._id", any(String.class));
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].errorCode", is("DOCUMENT_REPLACE_DIFFERENT_DOCID"))
          .body(
              "errors[0].message",
              is("The replace document and document resolved using filter have different _id"));
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(document))
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
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  @Order(2)
  class FindOneAndReplaceWithProjection {
    @Test
    public void byIdProjectionAfter() {
      insertDoc(
          """
                {
                  "_id": "docProjAfter",
                  "username": "userP",
                  "active_user" : true
                }
                """);

      String expectedAfterProjection =
          """
                {
                  "_id": "docProjAfter",
                  "status" : false
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "docProjAfter"},
                    "options" : {"returnDocument" : "after"},
                    "projection" : { "active_user":1, "status":1 },
                    "replacement" : { "username": "userP", "status" : false }
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expectedAfterProjection))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedAfterReplace =
          """
                {
                  "_id": "docProjAfter",
                  "username": "userP",
                  "status" : false
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "find": {
                    "filter" : {"_id" : "docProjAfter"}
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedAfterReplace));
    }

    @Test
    public void byIdProjectionBefore() {
      insertDoc(
          """
                {
                  "_id": "docProjBefore",
                  "username": "userP",
                  "active_user" : true
                }
                """);

      String expectedWithProjectionBefore =
          """
                {
                  "_id": "docProjBefore",
                  "active_user" : true
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "findOneAndReplace": {
                    "filter" : {"_id" : "docProjBefore"},
                    "options" : {"returnDocument" : "before"},
                    "projection" : { "active_user":1, "status":1 },
                    "replacement" : { "username": "userP", "status" : false }
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", jsonEquals(expectedWithProjectionBefore))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      String expectedAfterReplace =
          """
                {
                  "_id": "docProjBefore",
                  "username": "userP",
                  "status" : false
                }
                """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "find": {
                    "filter" : {"_id" : "docProjBefore"}
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expectedAfterReplace));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneAndReplaceIntegrationTest.super.checkMetrics("FindOneAndReplaceCommand");
    }
  }
}
