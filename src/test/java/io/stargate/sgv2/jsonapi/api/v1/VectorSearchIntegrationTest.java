package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
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
public class VectorSearchIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  private static final String collectionName = "my_collection";
  private static final String bigVectorCollectionName = "big_vector_collection";
  private static final String vectorSizeTestCollectionName = "vector_size_test_collection";

  // Just has to be bigger than maximum array size (1000)
  private static final int BIG_VECTOR_SIZE = 1536;

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void happyPathVectorSearch() {
      String json =
          """
                      {
                        "createCollection": {
                          "name" : "my_collection",
                          "options": {
                            "vector": {
                              "dimension": 5,
                              "metric": "cosine"
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    public void happyPathVectorSearchDefaultFunction() {
      String json =
          """
            {
              "createCollection": {
                "name" : "my_collection_default_function",
                "options": {
                  "vector": {
                    "dimension": 5
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(1));
    }

    @Test
    public void happyPathBigVectorCollection() {
      createVectorCollection(namespaceName, bigVectorCollectionName, BIG_VECTOR_SIZE);
    }

    @Test
    public void failForTooBigVector() {
      final int maxDimension = DocumentLimitsConfig.DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH;
      final int tooHighDimension = maxDimension + 10;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
            {
              "createCollection": {
                "name" : "TooBigVectorCollection",
                "options": {
                  "vector": {
                    "dimension": %d,
                    "metric": "cosine"
                  }
                }
              }
            }
            """
                  .formatted(tooHighDimension))
          .when()
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(nullValue()))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("VECTOR_SEARCH_TOO_BIG_VALUE"))
          .body(
              "errors[0].message",
              startsWith(
                  "Vector embedding property '$vector' length too big: "
                      + tooHighDimension
                      + " (max "
                      + maxDimension
                      + ")"));
    }

    @Test
    public void failForInvalidVectorMetric() {
      String json =
          """
                     {
                      "createCollection": {
                        "name" : "invalidVectorMetric",
                        "options": {
                          "vector": {
                            "dimension": 5,
                            "metric": "invalidName"
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
          .post(NamespaceResource.BASE_PATH, namespaceName)
          .then()
          .statusCode(200)
          .body("status.ok", is(nullValue()))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body(
              "errors[0].message",
              containsString(
                  "Problem: function name can only be 'dot_product', 'cosine' or 'euclidean'"));
    }
  }

  @Nested
  @Order(2)
  class InsertOneCollection {
    @Test
    public void insertVectorSearch() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "1",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you",
                  "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
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
          .body("status.insertedIds[0]", is("1"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
        {
          "find": {
            "filter" : {"_id" : "1"}
          }
        }
        """;
      String expected =
          """
        {
          "_id": "1",
          "name": "Coded Cleats",
          "description": "ChatGPT integrated sneakers that talk to you",
          "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    // Test to verify vector embedding size can exceed general Array length limit
    @Test
    public void insertBigVectorThenSearch() {
      final String vectorStr = buildVectorElements(1, BIG_VECTOR_SIZE);

      insertBigVectorDoc("bigVector1", "Victor", "Big Vectors Rule ok?", vectorStr);

      // Then verify it was inserted correctly
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
            {
              "find": {
                "filter" : {"_id" : "bigVector1"}
              }
            }
            """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVector1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", hasSize(BIG_VECTOR_SIZE));

      // And finally search for it (with different vector)
      final String vectorSearchStr = buildVectorElements(3, BIG_VECTOR_SIZE);
      final String findRequest =
          """
                      {
                        "find": {
                          "sort" : {"$vector" : [%s]},
                          "projection" : {"_id" : 1, "$vector" : 1}
                        }
                      }
                      """
              .formatted(vectorSearchStr);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findRequest)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVector1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", hasSize(BIG_VECTOR_SIZE));
    }

    @Test
    public void insertVectorCollectionWithoutVectorData() {
      String json =
          """
        {
           "insertOne": {
              "document": {
                  "_id": "10",
                  "name": "Coded Cleats",
                  "description": "ChatGPT integrated sneakers that talk to you"
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
          .body("status.insertedIds[0]", is("10"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "10"}
                        }
                      }
                      """;
      String expected =
          """
                      {
                        "_id": "10",
                        "name": "Coded Cleats",
                        "description": "ChatGPT integrated sneakers that talk to you"
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void insertEmptyVectorData() {
      String json =
          """
                      {
                         "insertOne": {
                            "document": {
                                "_id": "Invalid",
                                "name": "Coded Cleats",
                                "description": "ChatGPT integrated sneakers that talk to you",
                                "$vector": []
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
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vector value can't be empty"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void insertInvalidVectorData() {
      String json =
          """
                      {
                         "insertOne": {
                            "document": {
                                "_id": "Invalid",
                                "name": "Coded Cleats",
                                "description": "ChatGPT integrated sneakers that talk to you",
                                "$vector": [0.11, "abc", true, null]
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
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vector value needs to be array of numbers"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(3)
  class InsertManyCollection {
    @Test
    public void insertVectorSearch() {
      String json =
          """
                      {
                         "insertMany": {
                            "documents": [
                              {
                                "_id": "2",
                                "name": "Logic Layers",
                                "description": "An AI quilt to help you sleep forever",
                                "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
                              },
                              {
                                "_id": "3",
                                "name": "Vision Vector Frame",
                                "description": "Vision Vector Frame', 'A deep learning display that controls your mood",
                                "$vector": [0.12, 0.05, 0.08, 0.32, 0.6]
                              }
                            ],
                            "options" : {
                              "ordered" : true
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
          .body("status.insertedIds[0]", is("2"))
          .body("status.insertedIds[1]", is("3"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "2"}
                        }
                      }
                      """;
      String expected =
          """
                      {
                          "_id": "2",
                          "name": "Logic Layers",
                          "description": "An AI quilt to help you sleep forever",
                          "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
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
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }
  }

  public void insertVectorDocuments() {
    String json =
        """
            {
              "deleteMany": {
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
        .body("errors", is(nullValue()))
        .extract()
        .path("status.moreData");

    json =
        """
                    {
                       "insertMany": {
                          "documents": [
                            {
                              "_id": "1",
                              "name": "Coded Cleats",
                              "description": "ChatGPT integrated sneakers that talk to you",
                              "$vector": [0.1, 0.15, 0.3, 0.12, 0.05]
                             },
                             {
                               "_id": "2",
                               "name": "Logic Layers",
                               "description": "An AI quilt to help you sleep forever",
                               "$vector": [0.45, 0.09, 0.01, 0.2, 0.11]
                             },
                             {
                               "_id": "3",
                               "name": "Vision Vector Frame",
                               "description": "Vision Vector Frame', 'A deep learning display that controls your mood",
                               "$vector": [0.1, 0.05, 0.08, 0.3, 0.6]
                             }
                          ]
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
        // Sanity check: let's look for non-empty inserted id
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }

  @Nested
  @Order(4)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindCollection {

    @Test
    @Order(1)
    public void setUp() {
      insertVectorDocuments();
    }

    @Test
    @Order(2)
    public void happyPath() {
      String json =
          """
                      {
                        "find": {
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                          "projection" : {"_id" : 1, "$vector" : 1},
                          "options" : {
                              "limit" : 5
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
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$vector", is(notNullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(3)
    public void happyPathWithIncludeAll() {
      String json =
          """
                          {
                            "find": {
                              "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                              "projection" : {"*" : 1},
                              "options" : {
                                  "limit" : 5
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
          .body("errors", is(nullValue()))
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$vector", is(notNullValue()));
    }

    @Test
    @Order(4)
    public void happyPathWithExcludeAll() {
      String json =
          """
              {
                "find": {
                  "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                  "projection" : {"*" : 0},
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
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(2))
          .body("data.documents[0]", jsonEquals("{}"))
          .body("data.documents[1]", jsonEquals("{}"));
    }

    @Test
    @Order(5)
    public void happyPathWithFilter() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "1"},
                          "projection" : {"_id" : 1, "$vector" : 0},
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                          "options" : {
                              "limit" : 5
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
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(6)
    public void happyPathWithInFilter() {
      String json =
          """
            {
               "insertOne": {
                  "document": {
                      "_id": "xx",
                      "name": "Logic Layers",
                      "description": "ChatGPT integrated sneakers that talk to you",
                      "$vector": [0.25, 0.25, 0.25, 0.25, 0.25]
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
          .body("status.insertedIds[0]", is("xx"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
      json =
          """
                          {
                            "find": {
                              "filter" : {
                              "_id" : {"$in" : ["??", "xx"]},
                               "name":  {"$in" : ["Logic Layers","???"]}
                              },
                              "projection" : {"_id" : 1, "$vector" : 0},
                              "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                              "options" : {
                                  "limit" : 5
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
          .body("data.documents[0]._id", is("xx"))
          .body("data.documents[0].$vector", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(7)
    public void happyPathWithEmptyVector() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "1"},
                          "sort" : {"$vector" : []},
                          "options" : {
                              "limit" : 5
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
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].message", is(ErrorCode.SHRED_BAD_VECTOR_SIZE.getMessage()));
    }

    @Test
    @Order(8)
    public void happyPathWithInvalidData() {
      String json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "1"},
                          "sort" : {"$vector" : [0.11, "abc", true]},
                          "options" : {
                              "limit" : 5
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
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].message", is(ErrorCode.SHRED_BAD_VECTOR_VALUE.getMessage()));
    }

    @Test
    @Order(9)
    public void limitError() {
      String json =
          """
        {
          "find": {
            "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
            "projection" : {"_id" : 1, "$vector" : 1},
            "options" : {
                "limit" : 5000
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
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              endsWith("limit options should not be greater than 1000 for vector search."));
    }

    @Test
    @Order(10)
    public void skipError() {
      String json =
          """
        {
          "find": {
            "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
            "projection" : {"_id" : 1, "$vector" : 1},
            "options" : {
                "skip" : 10
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
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message", endsWith("skip options should not be used with vector search."));
    }
  }

  @Nested
  @Order(5)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FindOneCollection {
    @Test
    @Order(1)
    public void setUp() {
      insertVectorDocuments();
    }

    @Test
    @Order(2)
    public void happyPath() {
      String json =
          """
                      {
                        "findOne": {
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
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
          .body("data.document._id", is("3"))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(3)
    public void happyPathWithIdFilter() {
      String json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "1"},
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
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
          .body("data.document._id", is("1"))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(4)
    public void failWithEmptyVector() {
      String json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "1"},
                          "sort" : {"$vector" : []}
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
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].message", is(ErrorCode.SHRED_BAD_VECTOR_SIZE.getMessage()));
    }

    @Test
    @Order(4)
    public void failWithZerosVector() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"_id" : "1"},
            "sort" : {"$vector" : [0.0,0.0,0.0,0.0,0.0]}
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
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_QUERY"))
          .body(
              "errors[0].message",
              endsWith("Zero vectors cannot be indexed or queried with cosine similarity"));
    }

    @Test
    @Order(5)
    public void failWithInvalidVectorElements() {
      String json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "1"},
                          "sort" : {"$vector" : [0.11, "abc", true]}
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
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].message", is(ErrorCode.SHRED_BAD_VECTOR_VALUE.getMessage()));
    }

    // Vector columns can only use ANN, not regular filtering
    @Test
    @Order(6)
    public void failWithVectorFilter() {
      String json =
          """
                          {
                            "findOne": {
                              "filter" : {"$vector" : [ 1, 1, 1, 1, 1 ]}
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
          .body("errors", is(notNullValue()))
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"))
          .body(
              "errors[0].message",
              is(
                  "Cannot filter on '$vector' field using operator '$eq': only '$exists' is supported"));
    }
  }

  @Nested
  @Order(6)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UpdateCollection {
    @Test
    @Order(1)
    public void setUp() {
      insertVectorDocuments();
    }

    @Test
    @Order(2)
    public void setOperation() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id": "2"},
                          "update" : {"$set" : {"$vector" : [0.25, 0.25, 0.25, 0.25, 0.25]}},
                          "options" : {"returnDocument" : "after"}
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
          .body("data.document._id", is("2"))
          .body("data.document.$vector", contains(0.25f, 0.25f, 0.25f, 0.25f, 0.25f))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(3)
    public void unsetOperation() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"name": "Coded Cleats"},
                          "update" : {"$unset" : {"$vector" : null}},
                          "options" : {"returnDocument" : "after"}
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
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(nullValue()))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(4)
    public void setOnInsertOperation() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id": "11"},
                          "update" : {"$setOnInsert" : {"$vector": [0.11, 0.22, 0.33, 0.44, 0.55]}},
                          "options" : {"returnDocument" : "after", "upsert": true}
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
          .body("data.document._id", is("11"))
          .body("data.document.$vector", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is("11"))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(5)
    public void errorOperationForVector() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id": "3"},
                          "update" : {"$push" : {"$vector" : 0.33}},
                          "options" : {"returnDocument" : "after"}
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
          .body("errors", is(notNullValue()))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_VECTOR"))
          .body(
              "errors[0].message",
              is(ErrorCode.UNSUPPORTED_UPDATE_FOR_VECTOR.getMessage() + ": " + "$push"));
    }

    @Test
    @Order(6)
    public void setBigVectorOperation() {
      // First insert without a vector
      insertBigVectorDoc("bigVectorForSet", "Bob", "Desc for Bob.", null);

      // and verify we have null for it
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                                      {
                                        "find": {
                                          "filter" : {"_id" : "bigVectorForSet"}
                                        }
                                      }
                                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVectorForSet"))
          .body("data.documents[0].$vector", is(nullValue()));

      // then set the vector
      final String vectorStr = buildVectorElements(7, BIG_VECTOR_SIZE);
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id": "bigVectorForSet"},
                          "update" : {"$set" : {"$vector" : [ %s ]}},
                          "options" : {"returnDocument" : "after"}
                        }
                      }
                      """
              .formatted(vectorStr);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("bigVectorForSet"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.$vector", hasSize(BIG_VECTOR_SIZE))
          .body("errors", is(nullValue()));

      // and verify it was set to value with expected size
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                        {
                          "find": {
                            "filter" : {"_id" : "bigVectorForSet"}
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVectorForSet"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", hasSize(BIG_VECTOR_SIZE));
    }
  }

  @Nested
  @Order(7)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class VectorSearchExtendedCommands {
    @Test
    @Order(1)
    public void findOneAndUpdate() {
      insertVectorDocuments();
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                          "update" : {"$set" : {"status" : "active"}},
                          "options" : {"returnDocument" : "after"}
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
          .body("data.document._id", is("3"))
          .body("data.document.status", is("active"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(2)
    public void updateOne() {
      insertVectorDocuments();
      String json =
          """
                      {
                        "updateOne": {
                          "update" : {"$set" : {"new_col": "new_val"}},
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()))
          .body("errors", is(nullValue()));
      json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "3"}
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
          .body("data.document._id", is("3"))
          .body("data.document.new_col", is("new_val"));
    }

    @Test
    @Order(3)
    public void findOneAndReplace() {
      insertVectorDocuments();
      String json =
          """
                      {
                        "findOneAndReplace": {
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                          "replacement" : {"_id" : "3", "username": "user3", "status" : false, "$vector" : [0.12, 0.05, 0.08, 0.32, 0.6]},
                          "options" : {"returnDocument" : "after"}
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
          .body("data.document._id", is("3"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.username", is("user3"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(4)
    public void findOneAndReplaceWithoutVector() {
      insertVectorDocuments();
      String json =
          """
                      {
                        "findOneAndReplace": {
                          "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                          "replacement" : {"_id" : "3", "username": "user3", "status" : false},
                          "options" : {"returnDocument" : "after"}
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
          .body("data.document._id", is("3"))
          .body("data.document.$vector", is(nullValue()))
          .body("data.document.username", is("user3"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(5)
    public void findOneAndReplaceWithBigVector() {
      // First insert without a vector
      insertBigVectorDoc("bigVectorForFindReplace", "Alice", "Desc for Alice.", null);

      // and verify we have null for it
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
          {
            "find": {
              "filter" : {"_id" : "bigVectorForFindReplace"}
            }
          }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVectorForFindReplace"))
          .body("data.documents[0].$vector", is(nullValue()));

      // then set the vector
      final String vectorStr = buildVectorElements(2, BIG_VECTOR_SIZE);
      String json =
          """
                    {
                      "findOneAndReplace": {
                        "filter" : {"_id" : "bigVectorForFindReplace"},
                        "replacement" : {"_id" : "bigVectorForFindReplace", "$vector" : [ %s ]},
                        "options" : {"returnDocument" : "after"}
                      }
                    }
                    """
              .formatted(vectorStr);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("bigVectorForFindReplace"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.$vector", hasSize(BIG_VECTOR_SIZE))
          .body("errors", is(nullValue()));

      // and verify it was set to value with expected size
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "find": {
                          "filter" : {"_id" : "bigVectorForFindReplace"}
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("bigVectorForFindReplace"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", hasSize(BIG_VECTOR_SIZE));
    }

    @Test
    @Order(6)
    public void findOneAndDelete() {
      insertVectorDocuments();
      String json =
          """
            {
              "findOneAndDelete": {
                "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
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
          .body("data.document._id", is("3"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.name", is("Vision Vector Frame"))
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(7)
    public void deleteOne() {
      insertVectorDocuments();
      String json =
          """
            {
              "deleteOne": {
                "filter" : {"$vector" : {"$exists" : true}},
                "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
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
          .body("status.deletedCount", is(1))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the document
      json =
          """
        {
          "findOne": {
            "filter" : {"_id" : "3"}
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
          .body("data.document", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(8)
    public void insertVectorWithUnmatchedSize() {
      createVectorCollection(namespaceName, vectorSizeTestCollectionName, 5);
      // Insert data with $vector array size less than vector index defined size.
      final String vectorStrCount3 = buildVectorElements(0, 3);
      String jsonVectorStrCount3 =
          """
                      {
                         "insertOne": {
                            "document": {
                                "_id": "shortHandedVectorElements",
                                "name": "shortHandedVectorElements",
                                "description": "this document should have vector size as 5",
                                "$vector": [ %s ]
                            }
                         }
                      }
                      """
              .formatted(vectorStrCount3);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("Mismatched vector dimension"));

      // Insert data with $vector array size greater than vector index defined size.
      final String vectorStrCount7 = buildVectorElements(0, 7);
      String jsonVectorStrCount7 =
          """
                      {
                         "insertOne": {
                            "document": {
                                "_id": "excessVectorElements",
                                "name": "excessVectorElements",
                                "description": "this document should have vector size as 5",
                                "$vector": [ %s ]
                            }
                         }
                      }
                      """
              .formatted(vectorStrCount7);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount7)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("Mismatched vector dimension"));
    }

    @Test
    @Order(9)
    public void findVectorWithUnmatchedSize() {
      // Sort clause with $vector array size greater than vector index defined size.
      final String vectorStrCount3 = buildVectorElements(0, 3);
      String jsonVectorStrCount3 =
          """
                       {
                          "find": {
                            "sort" : {"$vector" : [ %s ]},
                            "options" : {
                                "limit" : 5
                            }
                          }
                        }
                        """
              .formatted(vectorStrCount3);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount3)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("Mismatched vector dimension"));

      // Insert data with $vector array size greater than vector index defined size.
      final String vectorStrCount7 = buildVectorElements(0, 7);
      String jsonVectorStrCount7 =
          """
                       {
                          "find": {
                            "sort" : {"$vector" : [ %s ]},
                            "options" : {
                                "limit" : 5
                            }
                          }
                        }
                        """
              .formatted(vectorStrCount7);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount7)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("errors", is(notNullValue()))
          .body("errors[0].message", endsWith("Mismatched vector dimension"));
    }
  }

  @Nested
  @Order(7)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class VectorSearchSimilarityProjection {

    @Test
    @Order(1)
    public void findOneSimilarityOption() {
      insertVectorDocuments();
      String json =
          """
            {
              "findOne": {
                "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                "options" : {"includeSimilarity" : true}
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
          .body("data.document._id", is("3"))
          .body("data.document.$similarity", notNullValue())
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(2)
    public void findSimilarityOption() {
      insertVectorDocuments();
      String json =
          """
        {
          "find": {
            "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
            "options" : {"includeSimilarity" : true}
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
          .body("errors", is(nullValue()))
          .body("data.documents", hasSize(3))
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$similarity", notNullValue())
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$similarity", notNullValue())
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$similarity", notNullValue());
    }
  }

  private static void createVectorCollection(
      String namespaceName, String collectionName, int vectorSize) {
    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(
            """
                            {
                              "createCollection": {
                                "name" : "%s",
                                "options": {
                                  "vector": {
                                    "dimension": %d,
                                    "metric": "cosine"
                                  }
                                }
                              }
                            }
                            """
                .formatted(collectionName, vectorSize))
        .when()
        .post(NamespaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("status.ok", is(1));
  }

  private void insertBigVectorDoc(String id, String name, String description, String vectorStr) {
    final String vectorDoc =
        """
                    {
                        "_id": "%s",
                        "name": "%s",
                        "description": "%s",
                        "$vector": %s
                    }
            """
            .formatted(
                id, name, description, (vectorStr == null) ? "null" : "[%s]".formatted(vectorStr));

    // First insert a document with a big vector

    given()
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
        .contentType(ContentType.JSON)
        .body(
            """
                            {
                               "insertOne": {
                                  "document": %s
                               }
                            }
                            """
                .formatted(vectorDoc))
        .when()
        .post(CollectionResource.BASE_PATH, namespaceName, bigVectorCollectionName)
        .then()
        .statusCode(200)
        .body("status.insertedIds[0]", is(id))
        .body("data", is(nullValue()))
        .body("errors", is(nullValue()));
  }

  private static String buildVectorElements(int offset, int count) {
    StringBuilder sb = new StringBuilder(count * 4);
    // Generate sequence with floating-point values that are exact in binary (like 0.5, 0.25)
    // so that conversion won't prevent equality matching
    final String[] nums = {"0.25", "0.5", "0.75", "0.975", "0.0125", "0.375", "0.625", "0.125"};
    for (int i = 0; i < count; ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      int ix = (offset + i) % nums.length;
      sb.append(nums[ix]);
    }
    return sb.toString();
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkInsertOneMetrics() {
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindCommand", JsonApiMetricsConfig.SortType.NONE.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndUpdateCommand", JsonApiMetricsConfig.SortType.NONE.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndUpdateCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndDeleteCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "UpdateOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
    }
  }
}
