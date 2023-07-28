package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
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
                "size": 5,
                "function": "cosine"
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
          .body("errors[0].message", startsWith("$vector field can't be empty"))
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
          .body("errors[0].message", startsWith("$vector search needs to be array of numbers"))
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
    String json = """
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
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[2]._id", is("1"))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(3)
    public void happyPathWithFilter() {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : "1"},
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
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(4)
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
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[1].message", is(ErrorCode.SHRED_BAD_VECTOR_SIZE.getMessage()));
    }

    @Test
    @Order(5)
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
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[1].message", is(ErrorCode.SHRED_BAD_VECTOR_VALUE.getMessage()));
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
    public void happyPathWithFilter() {
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
    public void happyPathWithEmptyVector() {
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
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[1].message", is(ErrorCode.SHRED_BAD_VECTOR_SIZE.getMessage()));
    }

    @Test
    @Order(5)
    public void happyPathWithInvalidData() {
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
          .body("errors[1].exceptionClass", is("JsonApiException"))
          .body("errors[1].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[1].message", is(ErrorCode.SHRED_BAD_VECTOR_VALUE.getMessage()));
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
    @Order(3)
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
    @Order(4)
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
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class VectorSearchExtendedCommands {
    @Test
    @Order(1)
    public void setUp() {
      insertVectorDocuments();
    }

    @Test
    @Order(2)
    public void findOneAndUpdate() {
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
    @Order(3)
    public void updateOne() {
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
          .body("data.document._id", is(3))
          .body("data.document.new_col", is("new_val"));
    }

    @Test
    @Order(3)
    public void findOneAndReplace() {
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
    public void findOneAndDelete() {
      String json =
          """
        {
          "findOneAndDelete": {
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
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(5)
    public void deleteOne() {
      String json =
          """
        {
          "deleteOne": {
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
            "filter" : {"_id" : "2"}
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
  }
}
