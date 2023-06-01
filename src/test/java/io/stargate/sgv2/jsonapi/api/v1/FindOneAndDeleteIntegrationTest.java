package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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
public class FindOneAndDeleteIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class FindOneAndDelete {
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

      String json =
          """
          {
            "findOneAndDelete": {
              "filter" : {"_id" : "doc3"}
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
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0));
    }

    @Test
    public void byIdNoData() {
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
            "findOneAndDelete": {
              "filter" : {"_id" : "doc5"}
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
          .body("status.deletedCount", is(0))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSortDesc() {
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

      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : -1}
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
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0));
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

      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : 1}
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
          .body("data.document", jsonEquals(document1))
          .body("status.deletedCount", is(1))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSortProjection() {
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
                  "username": "user2"
                }
                """;

      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : 1},
            "projection" : { "_id":0, "username":1 }
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
          .body("data.document", jsonEquals(expected))
          .body("status.deletedCount", is(1))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneAndDeleteIntegrationTest.super.checkMetrics("FindOneAndDeleteCommand");
    }
  }
}
