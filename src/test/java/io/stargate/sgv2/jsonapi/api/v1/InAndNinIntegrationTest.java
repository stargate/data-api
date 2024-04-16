package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import net.javacrumbs.jsonunit.ConfigurableJsonMatcher;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class InAndNinIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private void insert(String json) {
    given()
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
        .then()
        .statusCode(200);
  }

  private ConfigurableJsonMatcher[] getJsonEquals(int... docs) {
    String expected1 =
        """
                    {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                    """;
    String expected2 =
        """
                    { "_id": "doc2", "username": "user2", "subdoc" : { "id" : "abc" }, "array" : [ "value1" ] }
                    """;
    String expected3 =
        """
                    { "_id": "doc3", "username": "user3", "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true], "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]] }
                            """;
    String expected4 =
        """
                    { "_id": "doc4", "username" : "user4", "indexedObject" : { "0": "value_0", "1": "value_1" } }
                            """;
    String expected5 =
        """
                    { "_id": "doc5", "username": "user5", "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } } }
                            """;
    String expected6 =
        """
                    { "_id": {"$date": 6}, "username": "user6" }
                            """;

    ConfigurableJsonMatcher[] result = new ConfigurableJsonMatcher[docs.length];
    for (int i = 0; i < docs.length; i++) {
      int doc = docs[i];
      if (doc == 1) {
        result[i] = (jsonEquals(expected1));
      } else if (doc == 2) {
        result[i] = (jsonEquals(expected2));
      } else if (doc == 3) {
        result[i] = (jsonEquals(expected3));
      } else if (doc == 4) {
        result[i] = (jsonEquals(expected4));
      } else if (doc == 5) {
        result[i] = (jsonEquals(expected5));
      } else if (doc == 6) {
        result[i] = (jsonEquals(expected6));
      }
    }
    return result;
  }

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
                                    "date" : {"$date": 1672531200000},
                                    "age" : 20,
                                    "null_column": null
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
                                    "username": "user6"
                                  }
                                }
                              }
                            """);
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(2)
  class In {

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
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      String expected2 =
          """
                          {"_id":"doc4", "username":"user4", "indexedObject":{"0":"value_0","1":"value_1"}}
                          """;

      given()
          .headers(getHeaders())
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
    public void inConditionWithSubDoc() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"sub_doc" : {"$in" : [{ "a": 5, "b": { "c": "v1", "d": false }}]} }
                                }
                              }
                  """;

      String expected5 =
          """
                      {
                        "_id": "doc5",
                        "username": "user5",
                        "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                      }
                     """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected5));
    }

    @Test
    public void inConditionWithArray() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"array" : {"$in" : [["value1"]] } }
                                }
                              }
                  """;

      String expected =
          """
                          {"_id":"doc2", "username":"user2", "subdoc":{"id":"abc"},"array":["value1"]}
                          """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected));
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
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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
    public void idInConditionEmptyArray() {
      String json =
          """
                          {
                            "find": {
                              "filter" : {"_id" : {"$in": []}}
                            }
                          }
                          """;

      given()
          .headers(getHeaders())
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
    public void nonIDInConditionEmptyArray() {
      String json =
          """
                            {
                              "find": {
                                  "filter" : {
                                       "username" : {"$in" : []}
                                  }
                                }
                            }
                          """;
      given()
          .headers(getHeaders())
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
    public void nonIDInConditionEmptyArrayAnd() {
      String json =
          """
                            {
                              "find": {
                                  "filter" : {
                                    "$and": [
                                        {
                                            "age": {
                                                "$in": []
                                            }
                                        },
                                        {
                                            "username": "user1"
                                        }
                                    ]
                                  }
                                }
                            }
                          """;
      given()
          .headers(getHeaders())
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
    public void nonIDInConditionEmptyArrayOr() {
      String json =
          """
                            {
                              "find": {
                                  "filter" : {
                                    "$or": [
                                        {
                                            "age": {
                                                "$in": []
                                            }
                                        },
                                        {
                                            "username": "user1"
                                        }
                                    ]
                                  }
                                }
                            }
                          """;
      String expected1 =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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
          .headers(getHeaders())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", is("$in operator must have `ARRAY`"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"));
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
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(2))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 4)));
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
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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
    public void inConditionWithDuplicateValues() {
      String json =
          """
                            {
                              "find": {
                                  "filter" : {
                                       "username" : {"$in" : ["user1", "user1"]},
                                       "_id" : {"$in" : ["doc1", "???"]}
                                  }
                                }
                            }
                          """;
      String expected1 =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;
      given()
          .headers(getHeaders())
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
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(3)
  class Nin {

    @Test
    public void nonIdSimpleNinCondition() {
      String json =
          """
                          {
                            "find": {
                              "filter" : {"username" : {"$nin": ["user2", "user3","user4","user5","user6"]}}
                            }
                          }
                          """;

      String expected1 =
          """
                          {"_id":"doc1", "username":"user1", "active_user":true, "date" : {"$date": 1672531200000}, "age" : 20, "null_column": null}
                          """;

      given()
          .headers(getHeaders())
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
    public void ninConditionWithSubDoc() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"sub_doc" : {"$nin": [{ "a": 5, "b": { "c": "v1", "d": false } }]}}
                                }
                              }
                  """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          // except doc 5
          .body("data.documents", hasSize(5))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", notNullValue())
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 6)));
    }

    @Test
    public void ninConditionWithArray() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"array" : {"$nin" : [["value1"]] } }
                                }
                              }
                  """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          // except doc 2
          .body("data.documents", hasSize(5))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", notNullValue())
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 3, 4, 5, 6)));
    }

    @Test
    public void nonIdNinEmptyArray() {
      String json =
          """
                          {
                            "find": {
                              "filter" : {"username" : {"$nin": []}}
                            }
                          }
                          """;

      // should find everything
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(6))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
      ;
    }

    @Test
    public void idNinEmptyArray() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"_id" : {"$nin": []}}
                                }
                              }
                              """;

      // should find everything
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(6))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(4)
  class Combination {

    @Test
    public void nonIdInEmptyAndNonIdNinEmptyAnd() {
      String json =
          """
                          {
                            "find": {
                              "filter" : {"username" : {"$in": []}, "age": {"$nin" : []}}
                            }
                          }
                          """;

      // should find nothing
      given()
          .headers(getHeaders())
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
    public void nonIdInEmptyOrNonIdNinEmptyOr() {
      String json =
          """
                          {
                            "find": {
                              "filter" :{
                                "$or" :
                                [
                                {"username" : {"$in": []}},
                                {"age": {"$nin" : []}}
                                ]
                              }
                            }
                          }
                          """;

      // should find everything
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(6))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()))
          .body("data.documents", containsInAnyOrder(getJsonEquals(1, 2, 3, 4, 5, 6)));
    }

    @Test
    public void nonIdInEmptyAndIdNinEmptyAnd() {
      String json =
          """
                              {
                                "find": {
                                  "filter" : {"username" : {"$in": []}, "_id": {"$nin" : []}}
                                }
                              }
                              """;

      // should find nothing
      given()
          .headers(getHeaders())
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
  }
}
