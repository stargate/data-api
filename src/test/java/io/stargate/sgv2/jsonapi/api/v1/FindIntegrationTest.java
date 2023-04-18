package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindIntegrationTest extends CollectionResourceBaseIntegrationTest {

  // TODO refactor in https://github.com/stargate/jsonapi/issues/174
  //  - test names
  //  - order annotations
  //  - format json
  //  - errors field check
  //  - empty options test

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class Find {
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
          .statusCode(200);
    }

    @Test
    @Order(2)
    public void noFilter() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(5));
    }

    @Test
    @Order(2)
    public void noFilterWithOptions() {
      String json =
          """
            {
              "find": {
                "options" : {
                  "limit" : 1
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1));
    }

    @Test
    @Order(2)
    public void byId() {
      String json =
          """
            {
              "find": {
                "filter" : {"_id" : "doc1"}
              }
            }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void inCondition() {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : {"$in": ["doc1", "doc4"]}}
          }
        }
        """;
      // findOne resolves any one of the resolved documents. So the order of the documents in the
      // $in clause is not guaranteed.
      String expected1 = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      String expected2 =
          "{\"_id\":\"doc4\", \"indexedObject\":{\"0\":\"value_0\",\"1\":\"value_1\"}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(2))
          .body("data.docs", hasSize(2))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.docs", containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
    }

    @Test
    public void inConditionWithOtherCondition() {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : {"$in": ["doc1", "doc4"]}, "username" : "user1" }
          }
        }
        """;
      String expected1 = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(expected1));
    }

    @Test
    public void inConditionEmptyArray() {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : {"$in": []}}
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
          .body("data.count", is(0))
          .body("data.docs", hasSize(0))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inOperatorEmptyArrayWithAdditionalFilters() {
      String json =
          """
        {
          "find": {
            "filter" : {"username": "user1", "_id" : {"$in": []}}
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
          .body("data.count", is(0))
          .body("data.docs", hasSize(0))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void inConditionNonArrayArray() {
      String json =
          """
            {
              "find": {
                "filter" : {"_id" : {"$in": true}}
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
          .body("errors", is(notNullValue()))
          .body("errors[1].message", is("$in operator must have `ARRAY`"))
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    public void inConditionNonIdField() {
      String json =
          """
            {
              "find": {
                "filter" : {"non_id" : {"$in": ["a", "b", "c"]}}
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
          .body("errors", is(notNullValue()))
          .body("errors[1].message", is("Can use $in operator only on _id field"))
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    @Order(2)
    public void byIdWithProjection() {
      String json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "doc1"},
                    "projection": { "_id":0, "username":1 }
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals("{\"username\":\"user1\"}"));
    }

    @Test
    @Order(2)
    public void byColumn() {
      String json =
          """
            {
              "find": {
                "filter" : {"username" : "user1"}
              }
            }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqComparisonOperator() {
      String json =
          """
            {
              "find": {
                "filter" : {"username" : {"$eq" : "user1"}}
              }
            }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqSubDoc() {
      String json =
          """
            {
              "find": {
                "filter" : {"subdoc.id" : {"$eq" : "abc"}}
              }
            }
          """;
      String expected =
          "{\"_id\":\"doc2\", \"username\":\"user2\", \"subdoc\":{\"id\":\"abc\"},\"array\":[\"value1\"]}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqSubDocWithIndex() {
      String json =
          """
            {
              "find": {
                "filter" : {"indexedObject.1" : {"$eq" : "value_1"}}
              }
            }
          """;
      String expected =
          """
            {
                "_id": "doc4",
                "indexedObject" : { "0": "value_0", "1": "value_1" }
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqArrayElement() {
      String json =
          """
            {
              "find": {
                "filter" : {"array.0" : {"$eq" : "value1"}}
              }
            }
          """;
      String expected =
          """
            {
              "_id": "doc2",
              "username": "user2",
              "subdoc": {"id": "abc"},
              "array": ["value1"]
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withExistFalseOperator() {
      String json =
          """
            {
              "find": {
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
          .body("errors[1].message", is("$exists operator supports only true"))
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    @Order(2)
    public void withExistOperator() {
      String json =
          """
            {
              "find": {
                "filter" : {"active_user" : {"$exists" : true}}
              }
            }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withAllOperator() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$all" : ["tag1", "tag2"]}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withAllOperatorLongerString() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$all" : ["tag1", "tag1234567890123456789012345"]}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withAllOperatorMixedAFormatArray() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$all" : ["tag1", 1, true, null]}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withAllOperatorNoMatch() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withEqSubdocumentShortcut() {
      String json =
          """
            {
              "find": {
                "filter" : {"sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
              }
            }
          """;
      String expected =
          """
            {
              "_id": "doc5",
              "username": "user5",
              "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqSubdocument() {
      String json =
          """
            {
              "find": {
                "filter" : {"sub_doc" : { "$eq" : { "a": 5, "b": { "c": "v1", "d": false } } } }
              }
            }
          """;
      String expected =
          """
            {
              "_id": "doc5",
              "username": "user5",
              "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
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
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqSubdocumentOrderChangeNoMatch() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withEqSubdocumentNoMatch() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withSizeOperator() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$size" : 6}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withSizeOperatorNoMatch() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$size" : 1}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withEqOperatorArray() {
      String json =
          """
            {
              "find": {
                "filter" : {"tags" : {"$eq" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true]}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqOperatorNestedArray() {
      String json =
          """
            {
              "find": {
                "filter" : {"nestedArray" : {"$eq" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}}
              }
            }
          """;
      String expected =
          """
            {"_id": "doc3","username": "user3","tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]}
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void withEqOperatorArrayNoMatch() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withEqOperatorNestedArrayNoMatch() {
      String json =
          """
            {
              "find": {
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
          .body("errors", is(nullValue()))
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void withNEComparisonOperator() {
      String json =
          """
            {
              "find": {
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
    @Order(2)
    public void byBooleanColumn() {
      String json =
          """
            {
              "find": {
                "filter" : {"active_user" : true}
              }
            }
          """;
      String expected = "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.docs[0]", jsonEquals(expected));
    }
  }
}
