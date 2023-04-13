package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class CountIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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
    }

    private void insert(String json) {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()));
    }

    @Test
    public void noFilter() {
      String json =
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
          .body("status.count", is(5))
          .body("errors", is(nullValue()));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "countDocuments": {
              "options": {}
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
          .body("status.count", is(5))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byColumn() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : "user1"}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqComparisonOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : {"$eq" : "user1"}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDoc() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"subdoc.id" : {"$eq" : "abc"}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDocWithIndex() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"indexedObject.1" : {"$eq" : "value_1"}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqArrayElement() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"array.0" : {"$eq" : "value1"}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withExistFalseOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"active_user" : {"$exists" : false}}
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
          .body("errors[1].message", is("$exists operator supports only true"));
    }

    @Test
    public void withExistOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"active_user" : {"$exists" : true}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag2"]}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorLongerString() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", "tag1234567890123456789012345"]}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorMixedAFormatArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", 1, true, null]}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withAllOperatorNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$all" : ["tag1", 2, true, null]}}
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentShortcut() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDocument() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": false } } } }
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentOrderChangeNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "d": false, "c": "v1" } } } }
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqSubDocumentNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": true } } } }
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSizeOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$size" : 6}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSizeOperatorNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$size" : 1}}
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqOperatorArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true]}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqOperatorNestedArray() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqOperatorArrayNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1]}}
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withEqOperatorNestedArrayNoMatch() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null], ["abc"]]}}
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
          .body("status.count", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withNEComparisonOperator() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"username" : {"$ne" : "user1"}}
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
          .body("errors[1].message", startsWith("Unsupported filter operator $ne"));
    }

    @Test
    public void byBooleanColumn() {
      String json =
          """
          {
            "countDocuments": {
              "filter" : {"active_user" : true}
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
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }
}
