package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.blankString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class InsertIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  class InsertOne {

    @Test
    public void insertDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3"
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
          .body("status.insertedIds[0]", is("doc3"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """;
      String expected =
          """
          {
            "_id":"doc3",
            "username":"user3"
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
    public void insertDocumentWithDateValue() {
      String json =
          """
        {
          "insertOne": {
            "document": {
              "_id": "doc_date",
              "username": "doc_date_user3",
              "date_created": {"$date": 1672531200000}
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
          .body("status.insertedIds[0]", is("doc_date"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      String expected =
          """
        {
          "_id": "doc_date",
          "username": "doc_date_user3",
          "date_created": {"$date": 1672531200000}
        }
        """;

      String query_json =
          """
        {
          "find": {
            "filter" : {"_id" : "doc_date"}
          }
        }
        """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(query_json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(expected))
          .body("errors", is(nullValue()));
    }

    @Test
    public void insertDocumentWithDateDocId() {
      String json =
          """
            {
              "insertOne": {
                "document": {
                  "_id": {"$date": 1672539900000},
                  "username": "doc_date_user4",
                  "status": false
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
          .body("status.insertedIds[0]", jsonEquals("{\"$date\":1672539900000}"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      String expected =
          """
            {
              "_id": {"$date": 1672539900000},
              "username": "doc_date_user4",
              "status": false
            }
            """;

      String query_json =
          """
            {
              "find": {
                "filter" : {"_id" : {"$date": 1672539900000}}
              }
            }
            """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(query_json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void insertDocumentWithNumberId() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": 4,
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is(4))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : 4}
            }
          }
          """;
      String expected =
          """
          {
            "_id": 4,
            "username":"user4"
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
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3"
              },
              "options": {}
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
          .body("status.insertedIds[0]", is("doc3"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void noOptionsAllowed() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "docWithOptions"
                  },
                  "options": {"setting":"abc"}
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
          .body("errors", hasSize(2))
          .body("errors[0].message", startsWith("Command accepts no options: InsertOneCommand"))
          .body("errors[0].exceptionClass", is("JsonMappingException"))
          .body("errors[1].message", startsWith("Command accepts no options: InsertOneCommand"))
          .body("errors[1].errorCode", is("COMMAND_ACCEPTS_NO_OPTIONS"))
          .body("errors[1].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void insertDuplicateDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "duplicate",
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("duplicate"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "duplicate",
                "username": "different_user_name"
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
          .body("status.insertedIds", jsonEquals("[]"))
          .body(
              "errors[0].message",
              is(
                  "Failed to insert document with _id 'duplicate': Document already exists with the given _id"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "duplicate"}
            }
          }
          """;
      String expected =
          """
              {
                "_id": "duplicate",
                "username":"user4"
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void emptyDocument() {
      String json =
          """
          {
            "insertOne": {
              "document": {
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
          .body("status.insertedIds[0]", is(notNullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void notValidDocumentMissing() {
      String json =
          """
          {
            "insertOne": {
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
          .body("errors[0].message", is(not(blankString())))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"));
    }

    @Test
    public void tryInsertTooBigArray() {
      final ObjectMapper mapper = new ObjectMapper();
      // Max array elements allowed is 100; add a few more
      ObjectNode doc = mapper.createObjectNode();
      ArrayNode arr = doc.putArray("arr");
      for (int i = 0; i < 500; ++i) {
        arr.add(i);
      }
      final String json =
          """
              {
                "insertOne": {
                  "document": %s
                }
              }
              """
              .formatted(doc);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              is(
                  "Document size limitation violated: number of elements an Array has (500) exceeds maximum allowed (100)"));
    }

    @Test
    public void tryInsertTooLongName() {
      final ObjectMapper mapper = new ObjectMapper();
      // Max property name: 48 characters, let's try 100
      ObjectNode doc = mapper.createObjectNode();
      doc.put(
          "prop_12345_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789",
          72);
      final String json =
          """
                  {
                    "insertOne": {
                      "document": %s
                    }
                  }
                  """
              .formatted(doc);
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              is(
                  "Document size limitation violated: Property name length (100) exceeds maximum allowed (48)"));
    }
  }

  @Nested
  class InsertMany {

    @Test
    public void ordered() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .body("status.insertedIds", contains("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void orderedDuplicateIds() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4_duplicate"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .body("status.insertedIds", contains("doc4"))
          .body("data", is(nullValue()))
          .body("errors[0].message", startsWith("Failed to insert document with _id 'doc4'"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    public void orderedDuplicateDocumentNoNamespace() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
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
          .post(CollectionResource.BASE_PATH, "something_else", collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", is(empty()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith(
                  "Failed to insert document with _id 'doc4': INVALID_ARGUMENT: keyspace something_else does not exist"))
          .body("errors[0].exceptionClass", is("StatusRuntimeException"));
    }

    @Test
    public void unordered() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
                }
              ],
              "options": { "ordered": false }
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
          .body("status.insertedIds", containsInAnyOrder("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void unorderedDuplicateIds() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
                }
              ],
              "options": { "ordered": false }
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
          .body("status.insertedIds", containsInAnyOrder("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors[0].message", startsWith("Failed to insert document with _id 'doc4'"))
          .body("errors[0].errorCode", is("DOCUMENT_ALREADY_EXISTS"));

      json =
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.count", is(2))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withDifferentTypes() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "5",
                  "username": "user_id_5"
                },
                {
                  "_id": 5,
                  "username": "user_id_5_number"
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
          .body("status.insertedIds", contains("5", 5))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : "5"}
            }
          }
          """;
      String expected =
          """
          {
            "_id": "5",
            "username":"user_id_5"
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
          .body("data.documents[0]", jsonEquals(expected));

      json =
          """
          {
            "find": {
              "filter" : {"_id" : 5}
            }
          }
          """;
      expected =
          """
          {
            "_id": 5,
            "username":"user_id_5_number"
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {
                  "_id": "doc4",
                  "username": "user4"
                },
                {
                  "_id": "doc5",
                  "username": "user5"
                }
              ],
              "options": {}
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
          .body("status.insertedIds", contains("doc4", "doc5"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void emptyDocuments() {
      String json =
          """
          {
            "insertMany": {
              "documents": [
                {},
                {}
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
          .body("status.insertedIds", hasSize(2))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }

  @AfterAll
  public void checkMetrics() {
    checkMetrics("InsertOneCommand");
    checkMetrics("InsertManyCommand");
  }
}
