package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.is;

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
public class FindAndUpdateIntegrationTest extends CollectionResourceBaseIntegrationTest {

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindAndUpdate {
    @Test
    @Order(2)
    public void findByIdAndSet() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "doc3",
                    "username": "user3",
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
                "findOneAndUpdate": {
                  "filter" : {"_id" : "doc3"},
                  "update" : {"$set" : {"active_user": false}}
                }
              }
              """;
      String expected = "{\"_id\":\"doc3\", \"username\":\"user3\", \"active_user\":true}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc3"));

      expected = "{\"_id\":\"doc3\", \"username\":\"user3\", \"active_user\":false}";
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
    @Order(2)
    public void findByColumnAndSet() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "doc4",
                    "username": "user4"
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
                "findOneAndUpdate": {
                  "filter" : {"username" : "user4"},
                  "update" : {"$set" : {"new_col": "new_val"}}
                }
              }
              """;
      String expected = "{\"_id\":\"doc4\", \"username\":\"user4\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc4"));

      expected = "{\"_id\":\"doc4\", \"username\":\"user4\", \"new_col\": \"new_val\"}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "doc4"}
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
    @Order(2)
    public void findByIdAndUnset() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "doc5",
                    "username": "user5",
                    "unset_col": "val"
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
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "doc5"},
                    "update" : {"$unset" : {"unset_col": ""}}
                  }
                }
                    """;
      String expected = "{\"_id\":\"doc5\", \"username\":\"user5\", \"unset_col\":\"val\"}";
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected))
          .body("status.updatedIds[0]", is("doc5"));

      expected = "{\"_id\":\"doc5\", \"username\":\"user5\"}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "doc5"}
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

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UpdateOne {
    @Test
    @Order(2)
    public void findByIdAndSet() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "update_doc1",
                    "username": "update_user3",
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
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc1"},
                  "update" : {"$set" : {"active_user": false}}
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
          .body("status.updatedIds[0]", is("update_doc1"));

      String expected =
          "{\"_id\":\"update_doc1\", \"username\":\"update_user3\", \"active_user\":false}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "update_doc1"}
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
    @Order(2)
    public void findByColumnAndSet() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "update_doc2",
                    "username": "update_user2"
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
                "updateOne": {
                  "filter" : {"username" : "update_user2"},
                  "update" : {"$set" : {"new_col": "new_val"}}
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
          .body("status.updatedIds[0]", is("update_doc2"));

      String expected =
          "{\"_id\":\"update_doc2\", \"username\":\"update_user2\", \"new_col\": \"new_val\"}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "update_doc2"}
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
    @Order(2)
    public void findByIdAndUnset() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "update_doc3",
                    "username": "update_user3",
                    "unset_col": "val"
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
                  "findOneAndUpdate": {
                    "filter" : {"_id" : "update_doc3"},
                    "update" : {"$unset" : {"unset_col": ""}}
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
          .body("status.updatedIds[0]", is("update_doc3"));

      String expected = "{\"_id\":\"update_doc3\", \"username\":\"update_user3\"}";
      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "update_doc3"}
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
    @Order(2)
    public void findByColumnAndSetArray() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "update_doc4",
                    "username": "update_user4"
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
                "updateOne": {
                  "filter" : {"username" : "update_user4"},
                  "update" : {"$set" : {"new_col": ["new_val", "new_val2"]}}
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
          .body("status.updatedIds[0]", is("update_doc4"));

      String expected =
          "{\"_id\":\"update_doc4\", \"username\":\"update_user4\", \"new_col\": [\"new_val\", \"new_val2\"]}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "update_doc4"}
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
    @Order(2)
    public void findByColumnAndSetSubDoc() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "update_doc5",
                    "username": "update_user5"
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
                "updateOne": {
                  "filter" : {"username" : "update_user5"},
                  "update" : {"$set" : {"new_col": {"sub_doc_col" : "new_val2"}}}
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
          .body("status.updatedIds[0]", is("update_doc5"));

      String expected =
          "{\"_id\":\"update_doc5\", \"username\":\"update_user5\", \"new_col\": {\"sub_doc_col\":\"new_val2\"}}";
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "update_doc5"}
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

  @Nested
  class UpdateOneWithPop {
    @Test
    @Order(2)
    public void findByColumnAndPop() {
      // Let's test 4 valid pop operations:
      insertDoc(
          """
                      {
                        "_id": "update_doc_pop",
                        "array1": [ 1, 2, 3 ],
                        "array2": [ 4, 5, 6 ],
                        "array3": [ ]
                      }
                      """);
      String updateBody =
          """
                      {
                        "updateOne": {
                          "filter" : {"_id" : "update_doc_pop"},
                          "update" : {
                            "$pop" : {
                              "array1": 1,
                              "array2": -1,
                              "array3": 1,
                              "array4": -1
                              }
                          }
                        }
                      }
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateBody)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.updatedIds[0]", is("update_doc_pop"));

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                  {
                    "find": {
                      "filter" : {"_id" : "update_doc_pop"}
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body(
              "data.docs[0]",
              jsonEquals(
                  """
                      {
                        "_id": "update_doc_pop",
                        "array1": [ 1, 2 ],
                        "array2": [ 5, 6 ],
                        "array3": [ ]
                      }
                      """));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UpdateOneWithPush {
    @Test
    @Order(2)
    public void findByColumnAndPush() {
      insertDoc(
          """
                  {
                    "_id": "update_doc_push",
                    "array": [ 2 ]
                  }
                  """);
      String json =
          """
                  {
                    "updateOne": {
                      "filter" : {"_id" : "update_doc_push"},
                      "update" : {"$push" : {"array": 13 }}
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
          .body("status.updatedIds[0]", is("update_doc_push"));

      String expected = "{\"_id\":\"update_doc_push\", \"array\": [2, 13]}";
      json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "update_doc_push"}
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
    @Order(2)
    public void findByColumnAndPushWithEach() {
      insertDoc(
          """
                  {
                    "_id": "update_doc_push_each",
                    "array": [ 1 ]
                  }
                  """);
      String json =
          """
                  {
                    "updateOne": {
                      "filter" : {"_id" : "update_doc_push_each"},
                      "update" : {
                          "$push" : {
                             "array": { "$each" : [ 2, 3 ] },
                             "newArray": { "$each" : [ true ] }
                          }
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
          .body("status.updatedIds[0]", is("update_doc_push_each"));

      String expected =
          """
            { "_id":"update_doc_push_each",
              "array": [1, 2, 3],
              "newArray": [true] }
            """;
      json =
          """
            {
              "find": {
                "filter" : {"_id" : "update_doc_push_each"}
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
    @Order(2)
    public void findByColumnAndPushWithEachAndPosition() {
      insertDoc(
          """
                      {
                        "_id": "update_doc_push_each_position",
                        "array": [ 1, 2, 3 ]
                      }
                      """);
      String json =
          """
                      {
                        "updateOne": {
                          "filter" : {"_id" : "update_doc_push_each_position"},
                          "update" : {
                              "$push" : {
                                 "array": { "$each" : [ 4, 5 ], "$position" : 2 },
                                 "newArray": { "$each" : [ 1, 2, 3 ], "$position" : -999 }
                              }
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
          .body("status.updatedIds[0]", is("update_doc_push_each_position"));

      String expected =
          """
                { "_id":"update_doc_push_each_position",
                  "array": [1, 2, 4, 5, 3],
                  "newArray": [1, 2, 3] }
                """;
      json =
          """
                {
                  "find": {
                    "filter" : {"_id" : "update_doc_push_each_position"}
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

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UpdateOneWithInc {
    @Test
    @Order(2)
    public void findByColumnAndInc() {
      insertDoc(
          """
                     {
                        "_id": "update_doc_inc",
                        "number": 123
                      }
                      """);
      String updateJson =
          """
                      {
                        "updateOne": {
                          "filter" : {"_id" : "update_doc_inc"},
                          "update" : {"$inc" : {"number": -4, "newProp" : 0.25 }}
                        }
                      }
                      """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.updatedIds[0]", is("update_doc_inc"));

      String expectedDoc = "{\"_id\":\"update_doc_inc\", \"number\": 119, \"newProp\": 0.25 }";
      String findJson =
          """
                        {
                          "find": {
                            "filter" : {"_id" : "update_doc_inc"}
                          }
                        }
                        """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expectedDoc));
    }
  }

  private void insertDoc(String docJson) {
    String doc =
        """
                {
                  "insertOne": {
                    "document": %s
                  }
                }
                """
            .formatted(docJson);

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(doc)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
        .then()
        .statusCode(200);
  }
}
