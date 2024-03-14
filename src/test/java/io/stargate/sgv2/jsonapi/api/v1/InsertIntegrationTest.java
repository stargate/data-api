package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class InsertIntegrationTest extends AbstractCollectionIntegrationTestBase {
  private final ObjectMapper MAPPER = new ObjectMapper();

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(1)
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
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "find": {
                  "filter" : {"_id" : "doc3"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
              {
                "_id":"doc3",
                "username":"user3"
              }
              """))
          .body("errors", is(nullValue()));
    }

    // [https://github.com/stargate/jsonapi/issues/521]: allow hyphens in property names
    @Test
    public void insertDocumentWithHyphenatedColumn() {
      String json =
          """
              {
                "insertOne": {
                  "document": {
                    "_id": "doc-hyphen",
                    "user-name": "user #1"
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
          .body("status.insertedIds[0]", is("doc-hyphen"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
              {
                "find": {
                  "filter" : {"_id" : "doc-hyphen"}
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
              {
                "_id": "doc-hyphen",
                "user-name": "user #1"
              }
              """))
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
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("Command accepts no options: InsertOneCommand"))
          .body("errors[0].errorCode", is("COMMAND_ACCEPTS_NO_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
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
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Request invalid: field 'command.document' value `null` not valid. Problem: must not be null"));
    }
  }

  @Nested
  @Order(2)
  class InsertOneConstraintChecking {
    private static final int MAX_ARRAY_LENGTH = DocumentLimitsConfig.DEFAULT_MAX_ARRAY_LENGTH;

    @Test
    public void insertBiggestValidArray() {
      ObjectNode doc = MAPPER.createObjectNode();
      doc.put(DocumentConstants.Fields.DOC_ID, "docWithBigArray");
      ArrayNode arr = doc.putArray("arr");
      for (int i = 0; i < MAX_ARRAY_LENGTH; ++i) {
        arr.add(i);
      }
      _verifyInsert("docWithBigArray", doc);
    }

    @Test
    public void tryInsertTooBigArray() {
      // Max array elements allowed is 1000; add a few more
      ObjectNode doc = MAPPER.createObjectNode();
      ArrayNode arr = doc.putArray("arr");
      final int ARRAY_LEN = MAX_ARRAY_LENGTH + 10;
      for (int i = 0; i < ARRAY_LEN; ++i) {
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
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              is(
                  "Document size limitation violated: number of elements an indexable Array (property 'arr') has ("
                      + ARRAY_LEN
                      + ") exceeds maximum allowed ("
                      + MAX_ARRAY_LENGTH
                      + ")"));
    }

    // Test for nested paths, to ensure longer paths work too.
    @Test
    public void insertLongestValidPath() {
      // Need to hard-code knowledge of defaults here: max path is 1000
      // Since periods are also counted, let's do 4 x 200 (plus 3 dots) == 803 chars
      ObjectNode doc = MAPPER.createObjectNode();
      ObjectNode prop1 = doc.putObject("a1234".repeat(40));
      ObjectNode prop2 = prop1.putObject("b1234".repeat(40));
      ObjectNode prop3 = prop2.putObject("c1234".repeat(40));
      prop3.put("d1234".repeat(40), 42);
      doc.put(DocumentConstants.Fields.DOC_ID, "docWithLongPath");
      _verifyInsert("docWithLongPath", doc);
    }

    @Test
    public void tryInsertTooLongPath() {
      // Max path: 100 characters. Exceed with 4 x 250 + 3
      ObjectNode doc = MAPPER.createObjectNode();
      ObjectNode prop1 = doc.putObject("a".repeat(250));
      ObjectNode prop2 = prop1.putObject("b".repeat(250));
      ObjectNode prop3 = prop2.putObject("c".repeat(250));
      prop3.put("d".repeat(250), true);
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
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith(
                  "Document size limitation violated: property path length (1003) exceeds maximum allowed (1000)"));
    }

    @Test
    public void insertLongestValidNumber() {
      final String LONGEST_NUM = "9".repeat(DocumentLimitsConfig.DEFAULT_MAX_NUMBER_LENGTH);
      ObjectNode doc = MAPPER.createObjectNode();
      doc.put(DocumentConstants.Fields.DOC_ID, "docWithLongNumber");
      doc.put("num", new BigInteger(LONGEST_NUM));
      _verifyInsert("docWithLongNumber", doc);
    }

    // For [json-api#726]: avoid "too long number" due to BigDecimal conversion from
    // Engineering/Scientific notation
    @Test
    public void insertLongScientificNumber() {
      final BigDecimal BIG_NUMBER = new BigDecimal("2.0635595263889274e-35");
      ObjectNode doc = MAPPER.createObjectNode();
      doc.put(DocumentConstants.Fields.DOC_ID, "docWithLongScientificNumber");
      doc.put("num", BIG_NUMBER);
      _verifyInsert("docWithLongScientificNumber", doc);
    }

    @Test
    public void tryInsertTooLongNumber() {
      // Max number length: 100; use 110
      String tooLongNumStr = "1234567890".repeat(11);
      String json =
          """
                    {
                      "insertOne": {
                        "document": {
                           "_id" : 123,
                           "bigNumber" : %s
                        }
                      }
                    }
                    """
              .formatted(tooLongNumStr);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith(
                  "Document size limitation violated: Number value length (110) exceeds the maximum allowed (100"));
    }

    @Test
    public void insertLongestValidString() {
      final int strLen = DocumentLimitsConfig.DEFAULT_MAX_STRING_LENGTH_IN_BYTES - 20;

      // Issue with SAI max String length should not require more than 1 doc, so:
      ObjectNode doc = MAPPER.createObjectNode();
      final String docId = "docWithLongString";
      doc.put(DocumentConstants.Fields.DOC_ID, docId);
      // 1M / 8k means at most 125 max length Strings; add 63 (with _id 64)
      for (int i = 0; i < 63; ++i) {
        doc.put("text" + i, createBigString(strLen));
      }
      _verifyInsert(docId, doc);
    }

    @Test
    public void tryInsertTooLongString() {
      final String tooLongString =
          createBigString(DocumentLimitsConfig.DEFAULT_MAX_STRING_LENGTH_IN_BYTES + 50);
      String json =
          """
                        {
                          "insertOne": {
                            "document": {
                               "_id" : 123,
                               "bigString" : "%s"
                            }
                          }
                        }
                        """
              .formatted(tooLongString);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith(
                  "Document size limitation violated: indexed String value (property 'bigString') length (8056 bytes) exceeds maximum allowed"));
    }

    private String createBigString(int minLen) {
      // Create random "words" of 7 characters each, and space
      StringBuilder sb = new StringBuilder(minLen + 8);
      do {
        sb.append(RandomStringUtils.randomAlphanumeric(7)).append(' ');
      } while (sb.length() < minLen);
      return sb.toString();
    }

    @Test
    public void insertLongButNotTooLongDoc() throws Exception {
      JsonNode bigDoc =
          createBigDoc("bigValidDoc", DocumentLimitsConfig.DEFAULT_MAX_DOCUMENT_SIZE - 20_000);
      _verifyInsert("bigValidDoc", bigDoc);

      // But in this case, let's also verify that we can find it via nested properties
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                          {
                            "find": {
                              "filter" : {"root8.subId": 8}
                            }
                          }
                          """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]", jsonEquals(bigDoc));
    }

    @Test
    public void tryInsertTooLongDoc() throws Exception {
      JsonNode bigDoc =
          createBigDoc("bigValidDoc", DocumentLimitsConfig.DEFAULT_MAX_DOCUMENT_SIZE + 100_000);
      String json =
          """
                        {
                          "insertOne": {
                            "document": %s
                          }
                        }
                        """
              .formatted(bigDoc);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message", startsWith("Document size limitation violated: document size ("))
          .body("errors[0].message", endsWith(") exceeds maximum allowed (4000000)"));
    }

    @Test
    public void tryInsertDocWithTooBigObject() {
      final ObjectNode tooManyPropsDoc = MAPPER.createObjectNode();
      tooManyPropsDoc.put("_id", 456);

      // Max indexed: 1000, add some more
      ObjectNode subdoc = tooManyPropsDoc.putObject("subdoc");
      for (int i = 0; i < 1001; ++i) {
        subdoc.put("prop" + i, i);
      }

      String json =
          """
              {
                "insertOne": {
                  "document": %s
                }
              }
              """
              .formatted(tooManyPropsDoc);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith("Document size limitation violated: number of properties"))
          .body(
              "errors[0].message",
              endsWith(
                  "indexable Object (property 'subdoc') has (1001) exceeds maximum allowed (1000)"));
    }

    @Test
    public void tryInsertDocWithTooManyProps() {
      final ObjectNode tooManyPropsDoc = MAPPER.createObjectNode();
      tooManyPropsDoc.put("_id", 123);

      // About 2100, just needs to be above 2000
      for (int i = 0; i < 40; ++i) {
        ObjectNode branch = tooManyPropsDoc.putObject("root" + i);
        for (int j = 0; j < 51; ++j) {
          branch.put("prop" + j, j);
        }
      }

      String json =
          """
                            {
                              "insertOne": {
                                "document": %s
                              }
                            }
                            """
              .formatted(tooManyPropsDoc);
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
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              startsWith("Document size limitation violated: total number of indexed properties ("))
          .body("errors[0].message", endsWith(" in document exceeds maximum allowed (2000)"));
    }

    private void _verifyInsert(String docId, JsonNode doc) {
      final String json =
          """
                  {
                    "insertOne": {
                      "document": %s
                    }
                  }
                  """
              .formatted(doc);
      // Insert has to succeed
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is(docId))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // But let's also verify doc can be fetched and is what we inserted
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "find": {
                          "filter": {"_id" : "%s"}
                        }
                      }
                      """
                  .formatted(docId))
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", is(nullValue()))
          .body("data.documents[0]", jsonEquals(doc.toString()));
    }
  }

  @Nested
  @Order(3)
  class InsertInMixedCaseCollection {
    private static final String COLLECTION_MIXED = "MyCollection";

    // Both mixed-case Collection name and a field with mixed-case name
    @Test
    public void insertOneWithMixedCaseField() {
      createSimpleCollection(COLLECTION_MIXED);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "insertOne": {
                          "document": {
                            "_id": "mixed1",
                            "userName": "userA"
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, COLLECTION_MIXED)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("mixed1"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(
              """
                              {
                                "find": {
                                  "filter" : {"userName" : "userA"}
                                }
                              }
                              """)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, COLLECTION_MIXED)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
                                      {
                                        "_id": "mixed1",
                                        "userName": "userA"
                                      }
                                      """))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  @Order(4)
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
          .post(CollectionResource.BASE_PATH, "something_else", collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds", is(nullValue()))
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith("The provided namespace does not exist: something_else"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
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
          .body("status.insertedIds", containsInAnyOrder("5", 5))
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
          .body("status.insertedIds", containsInAnyOrder("doc4", "doc5"))
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

  @Nested
  @Order(5)
  class InsertManyLimitsChecking {
    @Test
    public void tryInsertTooLongNumber() {
      // Max number length: 100; use 110
      String tooLongNumStr = "1234567890".repeat(11);

      String json =
          """
              {
                "insertMany": {
                  "documents": [
                    {
                      "_id": "manyTooLong1",
                      "value": 123
                    },
                    {
                      "_id": "manyTooLong2",
                       "value": %s
                    }
                  ]
                }
              }
              """
              .formatted(tooLongNumStr);

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body(
              "errors[0].message",
              startsWith("Document size limitation violated: Number value length"))
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"));
    }

    @Test
    public void insertBigButNotTooBigPayload() {
      final int bigSize = DocumentLimitsConfig.DEFAULT_MAX_DOCUMENT_SIZE - 50_000;
      // 5 x bit under 4 M should be just under limit
      String json =
          """
                      {
                        "insertMany": {
                          "documents": [
                            %s,
                            %s,
                            %s,
                            %s,
                            %s
                          ]
                        }
                      }
                      """
              .formatted(
                  createBigDoc("big1", bigSize).toString(),
                  createBigDoc("big2", bigSize).toString(),
                  createBigDoc("big3", bigSize).toString(),
                  createBigDoc("big4", bigSize).toString(),
                  createBigDoc("big5", bigSize).toString());
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200);
    }

    // Testing Quarkus max payload (20M); separate from Max Doc Size limit (4M)
    // (so need 6 documents to exceed)
    @Test
    public void tryInsertTooBigPayload() {
      final int bigSize = DocumentLimitsConfig.DEFAULT_MAX_DOCUMENT_SIZE - 50_000;
      String json =
          """
                  {
                    "insertMany": {
                      "documents": [
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                      ]
                    }
                  }
                  """
              .formatted(
                  createBigDoc("toobig1", bigSize).toString(),
                  createBigDoc("toobig2", bigSize).toString(),
                  createBigDoc("toobig3", bigSize).toString(),
                  createBigDoc("toobig4", bigSize).toString(),
                  createBigDoc("toobig5", bigSize).toString(),
                  createBigDoc("toobig6", bigSize).toString());
      // Either we get 413 (Payload Too Large) due to Quarkus limit, or,
      // java.net.SocketException: Broken Pipe
      // in latter case, it's via "sneaky throw", unfortunately, so catch
      // needs to be for Exception and only then verifying
      try {
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json)
            .when()
            .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
            .then()
            .statusCode(403);
      } catch (Exception e) {
        // Sneaky throw, must catch any Exception, verify type separately
        assertThat(e)
            .isInstanceOf(java.net.SocketException.class)
            .hasMessageStartingWith("Broken pipe");
      }
    }
  }

  @Nested
  @Order(6)
  class InsertManyFails {
    @Test
    public void insertManyWithTooManyDocuments() {
      ArrayNode docs = MAPPER.createArrayNode();
      final int MAX_DOCS = OperationsConfig.DEFAULT_MAX_DOCUMENT_INSERT_COUNT;

      // We need to both exceed doc count limit AND to create big enough payload to
      // trigger message truncation: 21 x 1k == 21kB should be enough
      final String TEXT_1K = "abcd 1234 ".repeat(100);

      for (int i = 0; i < MAX_DOCS + 1; ++i) {
        ObjectNode doc =
            MAPPER
                .createObjectNode()
                .put("_id", "doc" + i)
                .put("username", "user" + i)
                .put("text", TEXT_1K);
        docs.add(doc);
      }
      String json =
          """
          {
            "insertMany": {
              "documents": %s
            }
          }
          """
              .formatted(docs.toString());

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              endsWith(
                  "not valid. Problem: amount of documents to insert is over the max limit ("
                      + docs.size()
                      + " vs "
                      + MAX_DOCS
                      + ")."))
          .body("errors[0].message", containsString("[TRUNCATED from "));
    }
  }

  @Nested
  @Order(99)
  class Metrics {
    @Test
    public void checkInsertOneMetrics() {
      InsertIntegrationTest.super.checkMetrics("InsertOneCommand");
      InsertIntegrationTest.super.checkDriverMetricsTenantId();
    }

    @Test
    public void checkInsertManyMetrics() {
      InsertIntegrationTest.super.checkMetrics("InsertManyCommand");
    }
  }

  private JsonNode createBigDoc(String docId, int minDocSize) {
    // While it'd be cleaner to build JsonNode representation, checking for
    // size much more expensive so go low-tech instead
    StringBuilder sb = new StringBuilder(minDocSize + 1000);
    sb.append("{\"_id\":\"").append(docId).append('"');

    boolean bigEnough = false;

    // Since we add one property before loop, reduce max by 1.
    // Target is around 1 meg; can have at most 2000 properties, and for
    // big doc we don't want to exceed 1000 bytes per property.
    // So let's make properties arrays of 4 Strings to get there.
    final int ROOT_PROPS = 99;
    final int LEAF_PROPS = 20; // so it's slightly under 2000 total properties, max
    final String TEXT_1K = "abcd123 ".repeat(120); // 960 chars

    // Use double loop to create a document with a lot of properties, 2-level nesting
    for (int i = 0; i < ROOT_PROPS && !bigEnough; ++i) {
      sb.append(",\n\"root").append(i).append("\":{");
      // Add one short entry to simplify following loop
      sb.append("\n \"subId\":").append(i);
      for (int j = 0; j < LEAF_PROPS && !bigEnough; ++j) {
        sb.append(",\n \"sub").append(j).append("\":");
        sb.append('[');
        sb.append('"').append(TEXT_1K).append("\",");
        sb.append('"').append(TEXT_1K).append("\",");
        sb.append('"').append(TEXT_1K).append("\",");
        sb.append('"').append(TEXT_1K).append("\"");
        sb.append(']');
        bigEnough = sb.length() >= minDocSize;
      }
      sb.append("\n}");
    }
    sb.append("\n}");
    if (!bigEnough) {
      fail(
          "Failed to create a document big enough, size: "
              + sb.length()
              + " bytes; needed "
              + minDocSize);
    }

    try {
      return MAPPER.readTree(sb.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
