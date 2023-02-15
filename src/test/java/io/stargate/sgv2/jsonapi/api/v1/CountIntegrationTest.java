package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.hamcrest.Matchers.is;
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
                              "_id": "doc5",
                              "username": "user5",
                              "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
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
    public void countNoFilter() {
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
          .body("status.counted_documents", is(5));
    }

    @Test
    @Order(2)
    public void countByColumn() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqComparisonOperator() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqSubDoc() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqSubDocWithIndex() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqArrayElement() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithExistFalseOperator() {
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
          .body("errors[0].message", is("$exists is supported only with true option"));
    }

    @Test
    @Order(2)
    public void countWithExistOperator() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithAllOperator() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithAllOperatorLongerString() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithAllOperatorMixedAFormatArray() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithAllOperatorNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Test
    @Order(2)
    public void countWithEqSubdocumentShortcut() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqSubdocument() {
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
          .body("status.counted_documents", is(1));
    }

    @Order(2)
    public void countWithEqSubdocumentOrderChangeNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Test
    @Order(2)
    public void countWithEqSubdocumentNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Test
    @Order(2)
    public void countWithSizeOperator() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithSizeOperatorNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Test
    @Order(2)
    public void countWithEqOperatorArray() {
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
          .body("status.counted_documents", is(1));
    }

    @Order(2)
    public void countWithEqOperatorNestedArray() {
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
          .body("status.counted_documents", is(1));
    }

    @Test
    @Order(2)
    public void countWithEqOperatorArrayNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Order(2)
    public void countWithEqOperatorNestedArrayNoMatch() {
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
          .body("status.counted_documents", is(0));
    }

    @Test
    @Order(2)
    public void countWithNEComparisonOperator() {
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
    @Order(2)
    public void countByBooleanColumn() {
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
          .body("status.counted_documents", is(1));
    }
  }
}
