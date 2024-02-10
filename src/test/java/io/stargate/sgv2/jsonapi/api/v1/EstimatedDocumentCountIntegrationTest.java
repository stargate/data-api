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
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class EstimatedDocumentCountIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Count {

    @Test
    @Order(1)
    public void setUp() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc1",
                "username": "user1",
                "active_user" : true
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc2",
                "username": "user2",
                "subdoc" : {
                   "id" : "abc"
                },
                "array" : [
                    "value1"
                ]
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3",
                "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
                "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc4",
                "indexedObject" : { "0": "value_0", "1": "value_1" }
              }
            }
          }
          """;
      insert(json);

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc5",
                "username": "user5",
                "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
              }
            }
          }
          """;
      insert(json);

      json =
          """
              {
                "insertOne": {
                  "document": {}
                }
              }
              """;
      insert(json);
    }

    private void insert(String json) {
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
    public void noFilter() {
      String json =
          """
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
          .body("status.count", is(5))
          .body("status.moreData", is(true))
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
