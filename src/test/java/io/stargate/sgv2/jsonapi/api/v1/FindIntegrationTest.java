package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
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
          .body("data.count", is(1));
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

    @Test
    @Order(3)
    public void sort() {
      deleteAllDocuments();
      Map<Integer, String> sorted = getDocuments(25, true);
      insert(sorted);
      String json =
          """
        {
          "find": {
            "sort" : ["username"]
          }
        }
        """;

      List<String> expected = new ArrayList<>(20);
      final Iterator<Map.Entry<Integer, String>> iterator = sorted.entrySet().iterator();
      for (int i = 0; i < 20; i++) expected.add(iterator.next().getValue());

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", contains((expected)));
    }

    @Test
    @Order(3)
    public void sortWithSkipLimit() {
      deleteAllDocuments();
      Map<Integer, String> sorted = getDocuments(25, true);
      insert(sorted);
      String json =
          """
              {
                "find": {
                  "sort" : ["username"],
                  "options" : {"skip": 10, "limit" : 10}
                }
              }
              """;

      List<String> expected = new ArrayList<>(10);
      final Iterator<Map.Entry<Integer, String>> iterator = sorted.entrySet().iterator();
      for (int i = 0; i < 20; i++) {
        if (i >= 10) expected.add(iterator.next().getValue());
      }

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(10))
          .body("data.docs", contains((expected)));
    }

    @Test
    @Order(4)
    public void sortDescending() {
      deleteAllDocuments();
      Map<Integer, String> sorted = getDocuments(25, false);
      insert(sorted);
      String json =
          """
              {
                "find": {
                  "sort" : ["-username"]
                }
              }
              """;

      List<String> expected = new ArrayList<>(20);
      final Iterator<Map.Entry<Integer, String>> iterator = sorted.entrySet().iterator();
      for (int i = 0; i < 20; i++) expected.add(iterator.next().getValue());

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(20))
          .body("data.docs", contains((expected)));
    }

    private void insert(Map<Integer, String> documents) {
      documents.values().forEach(doc -> insertDoc(doc));
    }

    private Map<Integer, String> getDocuments(int countOfDocuments, boolean asc) {
      String json = "{\"_id\":\"doc%s\", \"username\":\"user%s\", \"active_user\":true}";
      Map<Integer, String> data =
          new TreeMap<Integer, String>(
              new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                  return asc ? o1.compareTo(o2) : o2.compareTo(o1);
                }
              });
      for (int docId = 1; docId <= countOfDocuments; docId++) {
        data.put(docId, json.formatted(docId, docId));
      }
      return data;
    }
  }
}
