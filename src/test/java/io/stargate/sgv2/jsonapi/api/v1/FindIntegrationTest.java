package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FindIntegrationTest extends CollectionResourceBaseIntegrationTest {

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
    public void findNoFilter() {
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
          .body("data.count", is(5));
    }

    @Test
    @Order(2)
    public void findNoFilterWithOptions() {
      String json =
          """
                                  {
                                    "find": {
                                      "options" : {
                                        "pageSize" : 1
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
          .body("data.count", is(1))
          .body("data.nextPageState", is(notNullValue()));
    }

    @Test
    @Order(2)
    public void findById() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findByColumn() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqComparisonOperator() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqSubDoc() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqSubDocWithIndex() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqArrayElement() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithExistFalseOperator() {
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
          .body("errors[0].message", is("$exists is supported only with true option"));
    }

    @Test
    @Order(2)
    public void findWithExistOperator() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithAllOperator() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithAllOperatorLongerString() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithAllOperatorMixedAFormatArray() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithAllOperatorNoMatch() {
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
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void findWithEqSubdocumentShortcut() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqSubdocument() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Order(2)
    public void findWithEqSubdocumentOrderChangeNoMatch() {
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
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void findWithEqSubdocumentNoMatch() {
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
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void findWithSizeOperator() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithSizeOperatorNoMatch() {
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
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void findWithEqOperatorArray() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Order(2)
    public void findWithEqOperatorNestedArray() {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findWithEqOperatorArrayNoMatch() {
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
          .body("data.count", is(0));
    }

    @Order(2)
    public void findWithEqOperatorNestedArrayNoMatch() {
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
          .body("data.count", is(0));
    }

    @Test
    @Order(2)
    public void findWithNEComparisonOperator() {
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
    public void findByBooleanColumn() {
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOne {
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
      ;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200);

      json =
          """
                                {
                                  "insertOne": {
                                    "document": {
                                      "_id": "doc2",
                                      "username": "user2"
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
          .statusCode(200);

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
    public void findOneNoFilter() {
      String json =
          """
                                  {
                                    "findOne": {
                                    }
                                  }
                                  """;
      String expected = "{\"username\": \"user1\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1));
    }

    @Test
    @Order(2)
    public void findOneById() {
      String json =
          """
                                  {
                                    "findOne": {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findOneByColumn() {
      String json =
          """
                                  {
                                    "findOne": {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findOneWithExistsOperator() {
      String json =
          """
                                {
                                  "findOne": {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findOneWithAllOperator() {
      String json =
          """
                                {
                                  "findOne": {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    @Order(2)
    public void findOneWithSizeOperator() {
      String json =
          """
                                {
                                  "findOne": {
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
          .body("data.count", is(1))
          .body("data.docs[0]", jsonEquals(expected));
    }
  }
}
