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
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Find {
    @Test
    @Order(1)
    public void setUp() {
      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "doc1",
                              "username": "user1",
                              "active_user" : true,
                              "date" : {"$date": 1672531200000}
                            }
                          }
                        }
                      """);

      insert(
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
                      """);

      insert(
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
                      """);

      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "doc4",
                              "username" : "user4",
                              "indexedObject" : { "0": "value_0", "1": "value_1" }
                            }
                          }
                        }
                      """);

      insert(
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
                      """);

      insert(
          """
                        {
                          "insertOne": {
                            "document": {
                              "_id": {"$date": 6},
                              "user-name": "user6"
                            }
                          }
                        }
                      """);
    }

    private void insert(String json) {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(6));
    }

    @Test
    public void noFilterWithOptions() {
      String json =
          """
                      {
                        "find": {
                          "options" : {
                            "limit" : 2
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(2));
    }

    @Test
    public void byId() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "doc1"}
                        }
                      }
                      """;

      String expected =
          """
                      {
                          "_id": "doc1",
                          "username": "user1",
                          "active_user" : true,
                          "date" : {"$date": 1672531200000}
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void byDateId() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : {"$date": 6 }}
                        }
                      }
                      """;

      String expected =
          """
                      {
                        "_id": {"$date": 6},
                        "user-name": "user6"
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
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
      String expected1 =
          """
                      {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
                      """;
      String expected2 =
          """
                      {"_id":"doc4", "username":"user4", "indexedObject":{"0":"value_0","1":"value_1"}}
                      """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(2))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
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
      String expected1 =
          "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true, \"date\" : {\"$date\": 1672531200000}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected1));
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0))
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
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
                            "filter" : {
                                 "username" : {"$in" : ["user1", "user10"]}
                            }
                          }
                      }
                      """;
      String expected1 =
          "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true, \"date\" : {\"$date\": 1672531200000}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected1));
    }

    @Test
    public void inConditionNonIdFieldMulti() {
      String json =
          """
                      {
                        "find": {
                            "filter" : {
                                 "username" : {"$in" : ["user1", "user4"]}
                            }
                          }
                      }
                      """;
      String expected1 =
          """
                      {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
                      """;
      String expected2 =
          """
                      {"_id":"doc4", "username":"user4", "indexedObject":{"0":"value_0","1":"value_1"}}
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(2))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(jsonEquals(expected1), jsonEquals(expected2)));
    }

    @Test
    public void inConditionNonIdFieldIdField() {
      String json =
          """
                        {
                          "find": {
                              "filter" : {
                                   "username" : {"$in" : ["user1", "user10"]},
                                   "_id" : {"$in" : ["doc1", "???"]}
                              }
                            }
                        }
                      """;
      String expected1 =
          "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true, \"date\" : {\"$date\": 1672531200000}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected1));
    }

    @Test
    public void inConditionNonIdFieldIdFieldSort() {
      String json =
          """
                        {
                          "find": {
                              "filter" : {
                                   "username" : {"$in" : ["user1", "user10"]},
                                   "_id" : {"$in" : ["doc1", "???"]}
                              },
                              "sort": { "username": -1 }
                            }
                        }
                      """;
      String expected1 =
          "{\"_id\":\"doc1\", \"username\":\"user1\", \"active_user\":true, \"date\" : {\"$date\": 1672531200000}}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected1));
    }

    @Test
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

      String expected = """
              {"username":"user1"}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void byColumn() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"username" : "user1"}
                        }
                      }
                      """;

      String expected =
          """
                      {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    // [https://github.com/stargate/jsonapi/issues/521]: allow hyphens in property names
    @Test
    public void byColumnWithHyphen() {
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                    "find": {
                      "filter" : {"user-name" : "user6"}
                    }
                  }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                  {
                    "_id": {"$date": 6},
                    "user-name": "user6"
                  }
                """));
    }

    @Test
    public void withEqComparisonOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"username" : {"$eq" : "user1"}}
            }
          }
          """;

      String expected =
          """
              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
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
          """
              {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
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
            "username":"user4",
            "indexedObject" : { "0": "value_0", "1": "value_1" }
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[1].message", is("$exists operator supports only true"))
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("INVALID_FILTER_EXPRESSION"));
    }

    @Test
    public void withExistOperator() {
      String json =
          """
          {
            "find": {
              "filter" : {"active_user" : {"$exists" : true}}
            }
          }
          """;

      String expected =
          """
              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withEqSubDocumentShortcut() {
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqSubDocument() {
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withEqSubDocumentOrderChangeNoMatch() {
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withEqSubDocumentNoMatch() {
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void withSizeOperatorNoMatch() {
      String json =
          """
          {
            "find": {
              "filter" : {"tags" : {"$size" : 1}}
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(0));
    }

    @Test
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors[1].message", startsWith("Unsupported filter operator $ne"));
    }

    @Test
    public void byBooleanColumn() {
      String json =
          """
          {
            "find": {
              "filter" : {"active_user" : true}
            }
          }
          """;

      String expected =
          """
              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    @Order(2)
    public void byDateColumn() {
      String json =
          """
          {
            "find": {
              "filter" : {"date" : {"$date": 1672531200000}}
            }
          }
          """;

      String expected =
          """
              {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
              """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void simpleOr() {
      String json =
          """
                {
                    "find": {
                        "filter": {
                            "$or": [
                                {"username" : "user1"},
                                {"username" : "user2"}
                            ]
                        }
                    }
                }

              """;

      String expected1 =
          """
                  {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
                  """;
      String expected2 =
          """
                  {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                  """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(2));
    }

    @Test
    public void nestedAndOr() {
      String json =
          """
{
    "find": {
        "filter": {
            "$or": [
                {
                    "username": "user1"
                },
                {
                    "$and": [
                        {
                            "username": "user3"
                        },
                        {
                            "subdoc.id": {
                                "$eq": "abc"
                            }
                        }
                    ]
                }
            ]
        }
    }
}

              """;

      String expected1 =
          """
                  {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}}
                  """;
      String expected2 =
          """
                  {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                  """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1));
    }

    @Test
    public void OrWithIdIn() {
      String json =
          """
                      {
                          "find": {
                              "filter": {
                                  "$or": [
                                      {
                                          "username": "user1"
                                      },
                                      {
                                          "username": {
                                              "$in": [
                                                  "user2",
                                                  "user3"
                                              ]
                                          }
                                      }
                                  ]
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
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(3));
    }
  }

  @Nested
  @Order(2)
  class Metrics {

    @Test
    public void checkMetrics() {
      FindIntegrationTest.super.checkMetrics("FindCommand");
    }
  }
}
