package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
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
public class VectorizeSearchIntegrationTest extends AbstractNamespaceIntegrationTestBase {

  private static final String collectionName = "my_collection_vectorize";

  private static final String collectionNameDenyAll = "my_collection_vectorize_deny";

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void happyPathVectorSearch() {
      String json =
          """
                {
                    "createCollection": {
                        "name": "my_collection_vectorize",
                        "options": {
                            "vector": {
                                "metric": "cosine",
                                "dimension": 5,
                                "service": {
                                    "provider": "custom",
                                    "modelName": "text-embedding-ada-002",
                                    "authentication": {
                                        "type": [
                                            "SHARED_SECRET"
                                        ],
                                        "secretName": "name_given_by_user"
                                    },
                                    "parameters": {
                                        "project_id": "test project"
                                    }
                                }
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

      json =
          """
                    {
                        "createCollection": {
                            "name": "my_collection_vectorize_deny",
                            "options": {
                                "vector": {
                                    "metric": "cosine",
                                    "dimension": 5,
                                    "service": {
                                        "provider": "custom",
                                        "modelName": "text-embedding-ada-002",
                                        "authentication": {
                                            "type": [
                                                "SHARED_SECRET"
                                            ],
                                            "secretName": "name_given_by_user"
                                        },
                                        "parameters": {
                                            "project_id": "test project"
                                        }
                                    }
                                },
                                "indexing": {
                                    "deny": ["*"]
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
                      "$vectorize": "ChatGPT integrated sneakers that talk to you"
                  }
               }
            }
            """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("status.insertedIds[0]", is("1"))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // verify the metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> vectorizeCallDurationMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("vectorize_call_duration_seconds")
                          && !line.startsWith("vectorize_call_duration_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"InsertOneCommand\""))
              .toList();

      assertThat(vectorizeCallDurationMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");

                      if (line.contains("_count")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(1.0);
                      }
                    });
              });

      List<String> vectorizeInputBytesMetrics =
          metrics.lines().filter(line -> line.startsWith("vectorize_input_bytes")).toList();
      assertThat(vectorizeInputBytesMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");

                      if (line.contains("_count")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(1.0);
                      }

                      if (line.contains("_sum")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(44.0);
                      }
                    });
              });

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionNameDenyAll)
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

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f))
          .body("errors", is(nullValue()));
    }

    @Test
    public void insertVectorArrayData() {
      String json =
          """
          {
             "insertOne": {
                "document": {
                    "_id": "Invalid",
                    "name": "Coded Cleats",
                    "description": "ChatGPT integrated sneakers that talk to you",
                    "$vectorize": [0.25, 0.25]
                }
             }
          }
          """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
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
          .body(
              "errors[0].message",
              startsWith(
                  "$vectorize value needs to be text value, issue in document at position 1"))
          .body("errors[0].errorCode", is("INVALID_VECTORIZE_VALUE_TYPE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    public void insertInvalidVectorizeData() {
      String json =
          """
          {
             "insertOne": {
                "document": {
                    "_id": "Invalid",
                    "name": "Coded Cleats",
                    "description": "ChatGPT integrated sneakers that talk to you",
                    "$vectorize": 55
                }
             }
          }
          """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
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
          .body(
              "errors[0].message", startsWith(ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.getMessage()))
          .body("errors[0].errorCode", is("INVALID_VECTORIZE_VALUE_TYPE"))
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
                  "$vectorize": "An AI quilt to help you sleep forever"
                },
                {
                  "_id": "3",
                  "name": "Vision Vector Frame",
                  "description": "A deep learning display that controls your mood",
                  "$vectorize": "A deep learning display that controls your mood"
                }
              ],
              "options" : {
                "ordered" : true
              }
           }
        }
        """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
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

      // verify the metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> vectorizeCallDurationMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("vectorize_call_duration_seconds")
                          && !line.startsWith("vectorize_call_duration_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"InsertManyCommand\""))
              .toList();

      assertThat(vectorizeCallDurationMetrics)
          .satisfies(
              lines -> {
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");

                      if (line.contains("_count")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(1.0);
                      }
                    });
              });

      List<String> vectorizeInputBytesMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("vectorize_input_bytes")
                          && line.contains("command=\"InsertManyCommand\""))
              .toList();
      assertThat(vectorizeInputBytesMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");

                      if (line.contains("_count")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(2.0);
                      }

                      if (line.contains("_sum")) {
                        String[] parts = line.split(" ");
                        String numericPart =
                            parts[parts.length - 1]; // Get the last part which should be the number
                        double value = Double.parseDouble(numericPart);
                        assertThat(value).isEqualTo(84.0);
                      }
                    });
              });

      json =
          """
        {
          "find": {
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
          .body("data.documents[0]._id", is("2"))
          .body("data.documents[0].$vector", is(notNullValue()))
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
                      "$vectorize": "ChatGPT integrated sneakers that talk to you"
                     },
                     {
                       "_id": "2",
                       "name": "Logic Layers",
                       "description": "An AI quilt to help you sleep forever",
                       "$vectorize": "An AI quilt to help you sleep forever"
                     },
                     {
                       "_id": "3",
                       "name": "Vision Vector Frame",
                       "description": "A deep learning display that controls your mood",
                       "$vectorize": "A deep learning display that controls your mood"
                     }
                  ]
               }
            }
            """;
    given()
        .headers(
            Map.of(
                HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                getAuthToken(),
                HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                CustomITEmbeddingProvider.TEST_API_KEY))
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
              "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
              "projection" : {"_id" : 1, "$vector" : 1, "$vectorize" : 1},
              "options" : {
                  "limit" : 5
              }
            }
          }
          """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vectorize", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f))
          .body("data.documents[1]._id", is("2"))
          .body("data.documents[1].$vector", is(notNullValue()))
          .body("data.documents[1].$vectorize", is(notNullValue()))
          .body("data.documents[1].$vector", contains(0.45f, 0.09f, 0.01f, 0.2f, 0.11f))
          .body("data.documents[2]._id", is("3"))
          .body("data.documents[2].$vector", is(notNullValue()))
          .body("data.documents[2].$vectorize", is(notNullValue()))
          .body("data.documents[2].$vector", contains(0.1f, 0.05f, 0.08f, 0.3f, 0.6f))
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
            "projection" : {"_id" : 1, "$vector" : 0},
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
            "options" : {
                "limit" : 5
            }
          }
        }
        """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
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
    @Order(5)
    public void happyPathWithInvalidData() {
      String json =
          """
        {
          "find": {
            "filter" : {"_id" : "1"},
            "sort" : {"$vectorize" : [0.11, 0.22, 0.33]},
            "options" : {
                "limit" : 5
            }
          }
        }
        """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vectorize search clause needs to be text value"))
          .body("errors[0].errorCode", is("SHRED_BAD_VECTORIZE_VALUE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }

    @Test
    @Order(6)
    public void vectorizeFilterDenyAll() {
      String json =
          """
            {
              "find": {
                "filter" : {"$vectorize" : {"$exists" : true}}
              }
            }
            """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionNameDenyAll)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vectorize", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f));
    }

    @Test
    @Order(7)
    public void vectorizeSortDenyAll() {
      String json =
          """
            {
              "find": {
                "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
              }
            }
            """;

      given()
          .headers(
              Map.of(
                  HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
                  getAuthToken(),
                  HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                  CustomITEmbeddingProvider.TEST_API_KEY))
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionNameDenyAll)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vectorize", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f));
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
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
    @Order(3)
    public void happyPathWithIdFilter() {
      String json =
          """
        {
          "findOne": {
            "filter" : {"_id" : "1"},
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
            "sort" : {"$vectorize" : []}
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
          .body("errors[0].errorCode", is("SHRED_BAD_VECTORIZE_VALUE"))
          .body("errors[0].message", is(ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage()));
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
            "update" : {"$set" : {"description" : "ChatGPT upgraded", "$vectorize" : "ChatGPT upgraded"}},
            "options" : {"returnDocument" : "after"}
          }
        }
        """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document._id", is("2"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.description", is("ChatGPT upgraded"))
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
                "update" : {"$unset" : {"$vectorize" : null}},
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
              "update" : {"$setOnInsert" : {"$vectorize": "New data updated"}},
              "options" : {"returnDocument" : "after", "upsert": true}
            }
          }
          """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
              "sort" : {"$vectorize" : "A deep learning display that controls your mood"},
              "update" : {"$set" : {"status" : "active"}},
              "options" : {"returnDocument" : "after"}
            }
          }
          """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """;
      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
              "filter" : {"_id" : "1"}
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
              "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
              "replacement" : {"_id" : "1", "username": "user1", "status" : false, "description" : "Updating new data", "$vectorize" : "Updating new data"},
              "options" : {"returnDocument" : "after"}
            }
          }
          """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.username", is("user1"))
          .body("data.document.description", is("Updating new data"))
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
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
            "replacement" : {"_id" : "1", "username": "user1", "status" : false},
            "options" : {"returnDocument" : "after"}
          }
        }
        """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(nullValue()))
          .body("data.document.username", is("user1"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("errors", is(nullValue()));
    }

    @Test
    @Order(6)
    public void findOneAndDelete() {
      insertVectorDocuments();
      String json =
          """
                {
                  "findOneAndDelete": {
                    "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
                  }
                }
                """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.name", is("Coded Cleats"))
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
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """;

      given()
          .header(
              HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
              getAuthToken(),
              HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
              CustomITEmbeddingProvider.TEST_API_KEY)
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
            "filter" : {"_id" : "1"}
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
