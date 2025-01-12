package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.UUID;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class VectorSearchIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }

    @Test
    public void happyPathBigVectorCollection() {
      createVectorCollection(keyspaceName, bigVectorCollectionName, BIG_VECTOR_SIZE);
    }

    @Test
    public void failForTooBigVector() {
      final int maxDimension = DocumentLimitsConfig.DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH;
      final int tooHighDimension = maxDimension + 10;
      given()
          .headers(getHeaders())
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
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("1"));

      json =
          """
        {
          "find": {
            "filter" : {"_id" : "1"},
            "projection": { "*": 1 }
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
    }

    // Test to verify vector embedding size can exceed general Array length limit
    @Test
    public void insertBigVectorThenSearch() {
      final String vectorStr = buildVectorElements(1, BIG_VECTOR_SIZE);

      insertBigVectorDoc("bigVector1", "Victor", "Big Vectors Rule ok?", vectorStr);

      // Then verify it was inserted correctly
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
            {
              "find": {
                "filter" : {"_id" : "bigVector1"},
                "projection": { "*": 1 }
              }
            }
            """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(findRequest)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("10"));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("status", jsonEquals("{'insertedIds':[]}"))
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("status", jsonEquals("{'insertedIds':[]}"))
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vector value needs to be array of numbers"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void insertSimpleBinaryVector() {
      final String id = UUID.randomUUID().toString();
      final float[] expectedVector = new float[] {0.25f, -1.5f, 0.00f, 0.75f, 0.5f};
      final String base64Vector = generateBase64EncodedBinaryVector(expectedVector);
      String doc =
              """
                  {
                    "_id": "%s",
                    "name": "aaron",
                    "$vector": {"$binary": "%s"}
                  }
              """
              .formatted(id, base64Vector);

      // insert the document
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is(notNullValue()));

      // get the document and verify the vector value
      Response response =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(
                  "{\"find\": { \"filter\" : {\"_id\" : \"%s\"}, \"projection\" : {\"$vector\" : 1}}}"
                      .formatted(id))
              .when()
              .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
              .then()
              .statusCode(200)
              .body("$", responseIsFindSuccess())
              .body("data.documents[0]", jsonEquals(doc))
              .extract()
              .response();

      // Extract the Base64 encoded string from the response
      String base64VectorFromResponse =
          response.jsonPath().getString("data.documents[0].$vector.$binary");

      // Verify the Base64 encoded binary string is equal to the original base64Vector string
      Assertions.assertEquals(base64VectorFromResponse, base64Vector);

      // Convert the byte array to a float array
      float[] decodedVector = decodeBase64BinaryVectorToFloatArray(base64VectorFromResponse);

      // Verify that the decoded float array is equal to the assertions vector
      Assertions.assertArrayEquals(expectedVector, decodedVector, 0.0001f);
    }

    @Test
    public void insertLargeDimensionBinaryVector() {
      // create collection
      createVectorCollection(
          keyspaceName,
          "large_binary_vector_collection",
          DocumentLimitsConfig.DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH);

      final String id = UUID.randomUUID().toString();
      float[] expectedVector =
          buildVectorElementsArray(1, DocumentLimitsConfig.DEFAULT_MAX_VECTOR_EMBEDDING_LENGTH);
      final String base64Vector = generateBase64EncodedBinaryVector(expectedVector);
      String doc =
              """
                  {
                    "_id": "%s",
                    "name": "aaron",
                    "$vector": {"$binary": "%s"}
                  }
              """
              .formatted(id, base64Vector);

      // insert the document
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "large_binary_vector_collection")
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is(notNullValue()));

      // get the document and verify the vector value
      Response response =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(
                  "{\"find\": { \"filter\" : {\"_id\" : \"%s\"}, \"projection\" : {\"$vector\" : 1}}}"
                      .formatted(id))
              .when()
              .post(CollectionResource.BASE_PATH, keyspaceName, "large_binary_vector_collection")
              .then()
              .statusCode(200)
              .body("$", responseIsFindSuccess())
              .body("data.documents[0]", jsonEquals(doc))
              .extract()
              .response();

      // Extract the Base64 encoded string from the response
      String base64VectorFromResponse =
          response.jsonPath().getString("data.documents[0].$vector.$binary");

      // Verify the Base64 encoded binary string is equal to the original base64Vector string
      Assertions.assertEquals(base64VectorFromResponse, base64Vector);

      // Convert the byte array to a float array
      float[] decodedVector = decodeBase64BinaryVectorToFloatArray(base64VectorFromResponse);

      // Verify that the decoded float array is equal to the assertions vector
      Assertions.assertArrayEquals(expectedVector, decodedVector, 0.0001f);
    }

    @Test
    public void failToInsertBinaryVectorWithInvalidBinaryString() {
      final String invalidBinaryString = "@#$%^&*()";
      String doc =
              """
                  {
                    "name": "aaron",
                    "$vector": {"$binary": "%s"}
                  }
              """
              .formatted(invalidBinaryString);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_BINARY_VECTOR_VALUE"))
          .body(
              "errors[0].message",
              is(
                  "Bad binary vector value to shred: Invalid content in EJSON $binary wrapper: not valid Base64-encoded String, problem: Cannot access contents of TextNode as binary due to broken Base64 encoding: Illegal character '@' (code 0x40) in base64 content"));
    }

    @Test
    public void failToInsertBinaryVectorWithInvalidBinaryValue() {
      String doc =
          """
                  {
                    "name": "aaron",
                    "$vector": {"$binary": 1234}
                  }
              """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_BINARY_VECTOR_VALUE"))
          .body(
              "errors[0].message",
              is(
                  "Bad binary vector value to shred: Unsupported JSON value type in EJSON $binary wrapper (NUMBER): only STRING allowed"));
    }

    @Test
    public void failToInsertBinaryVectorWithInvalidVectorObject() {
      String doc =
          """
                  {
                    "name": "aaron",
                    "$vector": {"binary": "PoAAAD6AAAA+gAAAPoAAAD6AAAA="}
                  }
                  """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_DOCUMENT_VECTOR_TYPE"))
          .body(
              "errors[0].message",
              is(
                  "Bad $vector document type to shred : The key for the $vector object must be '$binary'"));
    }

    @Test
    public void failToInsertBinaryVectorWithInvalidDecodedValue() {
      String doc =
          """
                      {
                        "name": "aaron",
                        "$vector": {"$binary": "1234"}
                      }
                  """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_BINARY_VECTOR_VALUE"))
          .body(
              "errors[0].message",
              is(
                  "Bad binary vector value to shred: binary value to decode is not a multiple of 4 bytes long (3 bytes)"));
    }

    @Test
    public void failToInsertBinaryVectorWithUnmatchedVectorDimension() {
      final float[] wrongVectorDimension = new float[] {0.25f, -1.5f, 0.00f};
      final String base64Vector = generateBase64EncodedBinaryVector(wrongVectorDimension);
      String doc =
              """
                  {
                    "name": "aaron",
                    "$vector": {"$binary": "%s"}
                  }
              """
              .formatted(base64Vector);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body("{ \"insertOne\": { \"document\": %s }}".formatted(doc))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("VECTOR_SIZE_MISMATCH"))
          .body(
              "errors[0].message",
              is(
                  "Length of vector parameter different from declared '$vector' dimension: root cause = (InvalidQueryException) Not enough bytes to read a vector<float, 5>"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("2"))
          .body("status.insertedIds[1]", is("3"));

      json =
          """
                      {
                        "find": {
                          "filter" : {"_id" : "2"},
                          "projection": { "*": 1 }
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", jsonEquals(expected));
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
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then()
        .statusCode(200)
        .body("$", responseIsStatusOnly())
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
        .headers(getHeaders())
        .contentType(ContentType.JSON)
        .body(json)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then()
        // Sanity check: let's look for non-empty inserted id
        .body("$", responseIsWriteSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$vector", is(notNullValue()));
    }

    @Test
    @Order(2)
    public void happyPathBinaryVector() {
      String vectorString =
          generateBase64EncodedBinaryVector(new float[] {0.15f, 0.1f, 0.1f, 0.35f, 0.55f});
      String json =
              """
            {
              "find": {
                "sort" : {"$vector" : {"$binary" : "%s" } },
                "projection" : {"_id" : 1, "$vector" : 1},
                "options" : {
                    "limit" : 5
                }
              }
            }
            """
              .formatted(vectorString);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$vector", is(notNullValue()));
    }

    @Test
    @Order(2)
    public void happyPathWithIncludeSortVectorOption() {
      String json =
          """
            {
              "find": {
                "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]},
                "projection" : {"_id" : 1, "$vector" : 1},
                "options" : {
                    "limit" : 5,
                    "includeSortVector" : true
                }
              }
            }
            """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$vector", is(notNullValue()))
          .body("status.sortVector", is(notNullValue()));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("xx"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("xx"))
          .body("data.documents[0].$vector", is(nullValue()));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].message", is("$vector value can't be empty"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].message", is("$vector value needs to be array of numbers"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("3"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("1"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_SIZE"))
          .body("errors[0].message", is("$vector value can't be empty"));
    }

    @Test
    @Order(5)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_QUERY"))
          .body(
              "errors[0].message",
              oneOf(
                  "Zero and near-zero vectors cannot be indexed or queried with cosine similarity",
                  "Zero vectors cannot be indexed or queried with cosine similarity"));
    }

    @Test
    @Order(6)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTOR_VALUE"))
          .body("errors[0].message", is("$vector value needs to be array of numbers"));
    }

    // Vector columns can only use ANN, not regular filtering
    @Test
    @Order(7)
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("INVALID_FILTER_EXPRESSION"))
          .body(
              "errors[0].message",
              containsString(
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
                          "projection": { "*": 1 },
                          "update" : {"$set" : {"$vector" : [0.25, 0.25, 0.25, 0.25, 0.25]}},
                          "options" : {"returnDocument" : "after"}
                        }
                      }
                      """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("2"))
          .body("data.document.$vector", contains(0.25f, 0.25f, 0.25f, 0.25f, 0.25f));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(nullValue()));
    }

    @Test
    @Order(4)
    public void setOnInsertOperation() {
      String json =
          """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id": "11"},
                          "projection": { "*": 1 },
                          "update" : {"$setOnInsert" : {"$vector": [0.11, 0.22, 0.33, 0.44, 0.55]}},
                          "options" : {"returnDocument" : "after", "upsert": true}
                        }
                      }
                      """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("11"))
          .body("data.document.$vector", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.upsertedId", is("11"));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", is(notNullValue()))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].errorCode", is("UNSUPPORTED_UPDATE_FOR_VECTOR"))
          .body(
              "errors[0].message",
              is("Cannot use operator with '$vector' property" + ": " + "$push"));
    }

    @Test
    @Order(6)
    public void setBigVectorOperation() {
      // First insert without a vector
      insertBigVectorDoc("bigVectorForSet", "Bob", "Desc for Bob.", null);

      // and verify we have null for it
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                {
                  "find": {
                    "filter" : {"_id" : "bigVectorForSet"},
                    "projection": { "*": 1 }
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
                          "projection": { "*": 1 },
                          "update" : {"$set" : {"$vector" : [ %s ]}},
                          "options" : {"returnDocument" : "after"}
                        }
                      }
                      """
              .formatted(vectorStr);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("bigVectorForSet"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.$vector", hasSize(BIG_VECTOR_SIZE));

      // and verify it was set to value with assertions size
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                        {
                          "find": {
                            "filter" : {"_id" : "bigVectorForSet"},
                            "projection": { "*": 1 }
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.status", is("active"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()));
      json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "3"}
                        }
                      }
                      """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
                          "projection": { "*": 1 },
                          "replacement" : {"_id" : "3", "username": "user3", "status" : false, "$vector" : [0.12, 0.05, 0.08, 0.32, 0.6]},
                          "options" : {"returnDocument" : "after"}
                        }
                      }
                      """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.username", is("user3"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.$vector", is(nullValue()))
          .body("data.document.username", is("user3"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(5)
    public void findOneAndReplaceWithBigVector() {
      // First insert without a vector
      insertBigVectorDoc("bigVectorForFindReplace", "Alice", "Desc for Alice.", null);

      // and verify we have null for it
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
          {
            "find": {
              "filter" : {"_id" : "bigVectorForFindReplace"},
              "projection": { "*": 1 }
            }
          }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
                        "projection": { "*": 1 },
                        "replacement" : {"_id" : "bigVectorForFindReplace", "$vector" : [ %s ]},
                        "options" : {"returnDocument" : "after"}
                      }
                    }
                    """
              .formatted(vectorStr);

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("data.document._id", is("bigVectorForFindReplace"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.$vector", hasSize(BIG_VECTOR_SIZE));

      // and verify it was set to value with assertions size
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(
              """
                      {
                        "find": {
                          "filter" : {"_id" : "bigVectorForFindReplace"},
                          "projection": { "*": 1 }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
                "projection": { "*": 1 },
                "sort" : {"$vector" : [0.15, 0.1, 0.1, 0.35, 0.55]}
              }
            }
            """;

      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.deletedCount", is(1))
          .body("data.document._id", is("3"))
          .body("data.document.name", is("Vision Vector Frame"))
          .body("data.document.$vector", is(notNullValue()));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    @Order(8)
    public void insertVectorWithUnmatchedSize() {
      createVectorCollection(keyspaceName, vectorSizeTestCollectionName, 5);
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount3)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("VECTOR_SIZE_MISMATCH"))
          .body(
              "errors[0].message",
              startsWith(
                  "Length of vector parameter different from declared '$vector' dimension: root cause ="));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount7)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWritePartialSuccess())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("VECTOR_SIZE_MISMATCH"))
          .body(
              "errors[0].message",
              startsWith(
                  "Length of vector parameter different from declared '$vector' dimension: root cause ="));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount3)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("VECTOR_SIZE_MISMATCH"))
          .body(
              "errors[0].message",
              startsWith(
                  "Length of vector parameter different from declared '$vector' dimension: root cause ="));

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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(jsonVectorStrCount7)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, vectorSizeTestCollectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("VECTOR_SIZE_MISMATCH"))
          .body(
              "errors[0].message",
              startsWith(
                  "Length of vector parameter different from declared '$vector' dimension: root cause ="));
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.$similarity", notNullValue());
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
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(3))
          .body("data.documents[0]._id", is("3"))
          .body("data.documents[0].$similarity", notNullValue())
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$similarity", notNullValue())
          .body("data.documents[2]._id", is("1"))
          .body("data.documents[2].$similarity", notNullValue());
    }
  }

  private void createVectorCollection(String namespaceName, String collectionName, int vectorSize) {
    given()
        .headers(getHeaders())
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
        .post(KeyspaceResource.BASE_PATH, namespaceName)
        .then()
        .statusCode(200)
        .body("$", responseIsDDLSuccess())
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
        .headers(getHeaders())
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
        .post(CollectionResource.BASE_PATH, keyspaceName, bigVectorCollectionName)
        .then()
        .statusCode(200)
        .body("$", responseIsWriteSuccess())
        .body("status.insertedIds[0]", is(id));
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

  private static float[] buildVectorElementsArray(int offset, int count) {
    float[] elements = new float[count];
    // Generate sequence with floating-point values that are exact in binary (like 0.5, 0.25)
    // so that conversion won't prevent equality matching
    final float[] nums = {0.25f, 0.5f, 0.75f, 0.975f, 0.0125f, 0.375f, 0.625f, 0.125f};
    for (int i = 0; i < count; ++i) {
      int ix = (offset + i) % nums.length;
      elements[i] = nums[ix];
    }
    return elements;
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
      VectorSearchIntegrationTest.checkIndexUsageMetrics("FindCommand", true);

      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS.name());
      VectorSearchIntegrationTest.checkIndexUsageMetrics("FindOneCommand", true);
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndUpdateCommand", JsonApiMetricsConfig.SortType.NONE.name());
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndUpdateCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkIndexUsageMetrics("FindOneAndUpdateCommand", true);
      VectorSearchIntegrationTest.checkVectorMetrics(
          "FindOneAndDeleteCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkIndexUsageMetrics("FindOneAndDeleteCommand", true);
      VectorSearchIntegrationTest.checkVectorMetrics(
          "UpdateOneCommand", JsonApiMetricsConfig.SortType.SIMILARITY_SORT.name());
      VectorSearchIntegrationTest.checkIndexUsageMetrics("UpdateOneCommand", true);
    }
  }
}
