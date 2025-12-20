package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class VectorizeSearchIntegrationTest extends AbstractKeyspaceIntegrationTestBase {

  private static final String collectionName = "my_collection_vectorize";

  private static final String collectionNameDenyAll = "my_collection_vectorize_deny";

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void happyPathVectorSearch() {
      givenHeadersPostJsonThenOk(
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
                                        "providerKey" : "shared_creds.providerKey"
                                    },
                                    "parameters": {
                                        "projectId": "test project"
                                    }
                                }
                            }
                        }
                    }
                }
                """)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      givenHeadersPostJsonThenOk(
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
                                            "providerKey" : "shared_creds.providerKey"
                                        },
                                        "parameters": {
                                            "projectId": "test project"
                                        }
                                    }
                                },
                                "indexing": {
                                    "deny": ["*"]
                                }
                            }
                        }
                    }
                    """)
          .body("$", responseIsDDLSuccess())
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

      // verify starting metrics (we cannot assume clean slate)
      final double initialCallCount, initialInputByteSum;
      {
        final String allMetrics = getAllMetrics();
        List<String> vectorizeCallMetrics =
            getVectorizeCallDurationMetrics("InsertOneCommand", allMetrics, -1);
        // Usually get 0.0 if no earlier calls, but maybe something else
        initialCallCount = findEmbeddingCountFromMetrics(vectorizeCallMetrics);

        List<String> vectorizeInputBytesMetrics =
            allMetrics.lines().filter(line -> line.startsWith("vectorize_input_bytes")).toList();
        // same here, may get 0.0 if no earlier calls
        initialInputByteSum = findEmbeddingSumFromMetrics(vectorizeInputBytesMetrics);
      }

      givenHeadersAndJson(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("1"));

      // verify the metrics
      final String allMetrics = getAllMetrics();
      List<String> vectorizeCallMetrics =
          getVectorizeCallDurationMetrics("InsertOneCommand", allMetrics, 3);
      double afterCallCount = findEmbeddingCountFromMetrics(vectorizeCallMetrics);
      assertThat(Math.round(afterCallCount - initialCallCount))
          .withFailMessage(
              "Expected after (%s) call count to be 1.0 higher than before (%s)",
              afterCallCount, initialCallCount)
          .isEqualTo(1L);

      List<String> vectorizeInputBytesMetrics =
          allMetrics.lines().filter(line -> line.startsWith("vectorize_input_bytes")).toList();
      double afterCallInputByteSum = findEmbeddingSumFromMetrics(vectorizeInputBytesMetrics);
      assertThat(Math.round(afterCallInputByteSum - initialInputByteSum))
          .withFailMessage(
              "Expected after (%s) input bytes to be 44.0 higher than before (%s)",
              afterCallInputByteSum, initialInputByteSum)
          .isEqualTo(44L);

      givenHeadersAndJson(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionNameDenyAll)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("1"));

      givenHeadersAndJson(
              """
            {
              "find": {
                "filter" : {"_id" : "1"},
                "projection": { "$vector": 1 }
              }
            }
            """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f));
    }

    @Test
    public void insertVectorArrayData() {
      givenHeadersAndJson(
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
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("INVALID_VECTORIZE_VALUE_TYPE"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              startsWith(
                  "$vectorize value needs to be text value: issue in document at position 1"));
    }

    @Test
    public void insertInvalidVectorizeData() {
      givenHeadersAndJson(
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
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].message", startsWith("$vectorize value needs to be text value"))
          .body("errors[0].errorCode", is("INVALID_VECTORIZE_VALUE_TYPE"))
          .body("errors[0].exceptionClass", is("JsonApiException"));
    }
  }

  @Nested
  @Order(3)
  class InsertManyCollection {
    @Test
    public void insertVectorSearch() {
      givenHeadersAndJson(
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
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("2"))
          .body("status.insertedIds[1]", is("3"));

      // verify the metrics
      final String allMetrics = getAllMetrics();
      List<String> vectorizeCallDurationMetrics =
          getVectorizeCallDurationMetrics("InsertManyCommand", allMetrics, 3);

      assertThat(vectorizeCallDurationMetrics)
          .satisfies(
              lines -> {
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"SINGLE-TENANT\"");

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
          allMetrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("vectorize_input_bytes")
                          && line.contains("command=\"InsertManyCommand\""))
              .toList();
      assertThat(vectorizeInputBytesMetrics)
          .satisfies(
              lines -> {
                // aaron, this used to check the number of lines, that is linked to the number of
                // percentiles and is very very fragle to include in a test
                lines.forEach(
                    line -> {
                      assertThat(line).contains("embedding_provider=\"CustomITEmbeddingProvider\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"SINGLE-TENANT\"");

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

      givenHeadersAndJson(
              """
        {
          "find": {
            "filter" : {"_id" : "2"},
            "projection": { "$vector": 1 }
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("2"))
          .body("data.documents[0].$vector", is(notNullValue()));
    }
  }

  public void insertVectorDocuments() {
    givenHeadersAndJson(
            """
      {
        "deleteMany": {
        }
      }
      """)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then()
        .statusCode(200)
        .body("$", responseIsStatusOnly())
        .extract()
        .path("status.moreData");

    givenHeadersAndJson(
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
            """)
        .when()
        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
        .then()
        // Sanity check: let's look for non-empty inserted id
        .statusCode(200)
        .body("$", responseIsWriteSuccess())
        .body("status.insertedIds[0]", not(emptyString()));
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
      givenHeadersAndJson(
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
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
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
          .body("data.documents[2].$vector", contains(0.1f, 0.05f, 0.08f, 0.3f, 0.6f));
    }

    @Test
    @Order(3)
    public void happyPathWithFilter() {
      givenHeadersAndJson(
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
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(nullValue()));
    }

    @Test
    @Order(5)
    public void happyPathWithInvalidData() {
      givenHeadersAndJson(
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
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("SORT_CLAUSE_VALUE_INVALID"))
          .body("errors[0].exceptionClass", is("SortException"))
          .body(
              "errors[0].message",
              startsWith(
                  "Value used for sort expression on path '$vectorize' not valid: vectorize sort expression needs to be non-blank String"));
    }

    @Test
    @Order(6)
    public void vectorizeSortDenyAll() {
      givenHeadersAndJson(
              """
            {
              "find": {
                "projection": { "$vector": 1, "$vectorize" : 1 },
                "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
              }
            }
            """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionNameDenyAll)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f))
          .body("data.documents[0].$vectorize", is(notNullValue()));
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
      givenHeadersAndJson(
              """
        {
          "findOne": {
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
            "options" : { "includeSortVector" : true }
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("1"))
          .body("status.sortVector", is(notNullValue()));
    }

    @Test
    @Order(3)
    public void happyPathWithIdFilter() {
      givenHeadersAndJson(
              """
        {
          "findOne": {
            "filter" : {"_id" : "1"},
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """)
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
      givenHeadersAndJson(
              """
        {
          "findOne": {
            "filter" : {"_id" : "1"},
            "sort" : {"$vectorize" : []}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].exceptionClass", is("SortException"))
          .body("errors[0].errorCode", is("SORT_CLAUSE_VALUE_INVALID"))
          .body(
              "errors[0].message",
              startsWith(
                  "Value used for sort expression on path '$vectorize' not valid: vectorize sort expression needs to be non-blank String"));
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
      givenHeadersAndJson(
              """
        {
          "findOneAndUpdate": {
            "filter" : {"_id": "2"},
            "projection": { "$vector": 1 },
            "update" : {"$set" : {"description" : "ChatGPT upgraded", "$vectorize" : "ChatGPT upgraded"}},
            "options" : {"returnDocument" : "after"}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("2"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.description", is("ChatGPT upgraded"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(3)
    public void unsetOperation() {
      givenHeadersAndJson(
              """
            {
              "findOneAndUpdate": {
                "filter" : {"name": "Coded Cleats"},
                "update" : {"$unset" : {"$vectorize" : null}},
                "options" : {"returnDocument" : "after"}
              }
            }
            """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(nullValue()))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(4)
    public void setOnInsertOperation() {
      givenHeadersAndJson(
              """
          {
            "findOneAndUpdate": {
              "filter" : {"_id": "11"},
              "projection": { "$vector": 1 },
              "update" : {"$setOnInsert" : {"$vectorize": "New data updated"}},
              "options" : {"returnDocument" : "after", "upsert": true}
            }
          }
          """)
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
  }

  @Nested
  @Order(7)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class VectorSearchExtendedCommands {
    @Test
    @Order(1)
    public void findOneAndUpdate_sortClause() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
          {
            "findOneAndUpdate": {
              "sort" : {"$vectorize" : "A deep learning display that controls your mood"},
              "update" : {"$set" : {"status" : "active"}},
              "options" : {"returnDocument" : "after"}
            }
          }
          """)
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
    public void findOneAndUpdate_updateClause() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
                  {
                    "findOneAndUpdate": {
                      "sort" : {"$vectorize" : "A deep learning display that controls your mood"},
                      "update" : {"$set" : {"status" : "active","$vectorize":"An AI quilt to help you sleep forever"}},
                      "options" : {"returnDocument" : "after"}
                    }
                  }
                  """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.status", is("active"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));

      givenHeadersAndJson(
              """
                        {
                          "findOne": {
                            "filter" : {"_id" : "3"},
                            "projection":{
                                "$vector":true, "$vectorize":true
                            }
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("3"))
          .body("data.document.$vectorize", is("An AI quilt to help you sleep forever"))
          .body("data.document.$vector", contains(0.45f, 0.09f, 0.01f, 0.2f, 0.11f))
          .body("data.document.status", is("active"));
    }

    @Test
    @Order(3)
    public void updateOne_sortClause() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
        {
          "updateOne": {
            "update" : {"$set" : {"new_col": "new_val"}},
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()));

      givenHeadersAndJson(
              """
          {
            "findOne": {
              "filter" : {"_id" : "1"}
            }
          }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("1"))
          .body("data.document.new_col", is("new_val"));
    }

    @Test
    @Order(4)
    public void updateOne_updateClause() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
                {
                  "updateOne": {
                    "update" : {"$set" : {"new_col": "new_val", "$vectorize":"ChatGPT upgraded"}},
                    "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", is(nullValue()));
      givenHeadersAndJson(
              """
                        {
                          "findOne": {
                            "filter" : {"_id" : "1"},
                            "projection":{
                                "$vector":true, "$vectorize":true
                            }
                          }
                        }
                        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("1"))
          .body("data.document.$vectorize", is("ChatGPT upgraded"))
          .body("data.document.$vector", contains(0.1f, 0.16f, 0.31f, 0.22f, 0.15f))
          .body("data.document.new_col", is("new_val"));
    }

    @Test
    @Order(5)
    public void findOneAndReplace() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
          {
            "findOneAndReplace": {
              "projection": { "$vector": 1 },
              "sort" : {"$vectorize" : "ChatGPT upgraded"},
              "replacement" : {"_id" : "1", "username": "user1", "status" : false, "description" : "Updating new data", "$vectorize" : "Updating new data"},
              "options" : {"returnDocument" : "after"}
            }
          }
          """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(notNullValue()))
          .body("data.document.username", is("user1"))
          .body("data.document.description", is("Updating new data"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(6)
    public void findOneAndReplaceWithoutVector() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
        {
          "findOneAndReplace": {
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
            "replacement" : {"_id" : "1", "username": "user1", "status" : false},
            "options" : {"returnDocument" : "after"}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document._id", is("1"))
          .body("data.document.$vector", is(nullValue()))
          .body("data.document.username", is("user1"))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(7)
    public void findOneAndDelete() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
                {
                  "findOneAndDelete": {
                    "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"},
                    "projection": { "*": 1 }
                  }
                }
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.deletedCount", is(1))
          .body("data.document._id", is("1"))
          .body("data.document.name", is("Coded Cleats"))
          .body("data.document.$vector", is(notNullValue()));
    }

    @Test
    @Order(8)
    public void deleteOne() {
      insertVectorDocuments();
      givenHeadersAndJson(
              """
        {
          "deleteOne": {
            "filter" : {"$vector" : {"$exists" : true}},
            "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // ensure find does not find the document
      givenHeadersAndJson(
              """
        {
          "findOne": {
            "filter" : {"_id" : "1"}
          }
        }
        """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    @Order(9)
    public void createDropDifferentVectorDimension() {
      givenHeadersAndJson(
              """
                  {
                      "createCollection": {
                          "name": "cacheTestTable",
                          "options": {
                              "vector": {
                                  "metric": "cosine",
                                  "dimension": 5,
                                  "service": {
                                      "provider": "custom",
                                      "modelName": "text-embedding-ada-002",
                                      "authentication": {
                                          "providerKey" : "shared_creds.providerKey"
                                      },
                                      "parameters": {
                                          "projectId": "test project"
                                      }
                                  }
                              }
                          }
                      }
                  }
                  """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // insertOne to trigger the schema cache
      givenHeadersAndJson(
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
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "cacheTestTable")
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("1"));

      // DeleteCollection, should evict the corresponding schema cache
      givenHeadersAndJson(
                  """
                          {
                            "deleteCollection": {
                              "name": "%s"
                            }
                          }
                          """
                  .formatted("cacheTestTable"))
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.ok", is(1));

      // Create a new collection with same name, but dimension as 6
      givenHeadersAndJson(
              """
                  {
                      "createCollection": {
                          "name": "cacheTestTable",
                          "options": {
                              "vector": {
                                  "metric": "cosine",
                                  "dimension": 6,
                                  "service": {
                                      "provider": "custom",
                                      "modelName": "text-embedding-ada-002",
                                      "authentication": {
                                          "providerKey" : "shared_creds.providerKey"
                                      },
                                      "parameters": {
                                          "projectId": "test project"
                                      }
                                  }
                              }
                          }
                      }
                  }
                  """)
          .when()
          .post(KeyspaceResource.BASE_PATH, keyspaceName)
          .then()
          .statusCode(200)
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));

      // insertOne, should use the new collectionSetting, since the outdated one has been evicted
      givenHeadersAndJson(
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
                """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "cacheTestTable")
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds[0]", is("1"));

      // find, verify the dimension
      givenHeadersAndJson(
              """
                    {
                      "find": {
                        "projection": { "$vector": 1, "$vectorize" : 1 },
                        "sort" : {"$vectorize" : "ChatGPT integrated sneakers that talk to you"}
                      }
                    }
                    """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, "cacheTestTable")
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(1))
          .body("data.documents[0]._id", is("1"))
          .body("data.documents[0].$vector", is(notNullValue()))
          .body("data.documents[0].$vector", contains(0.1f, 0.15f, 0.3f, 0.12f, 0.05f, 0.05f))
          .body("data.documents[0].$vectorize", is(notNullValue()));
    }
  }

  @Nested
  @Order(8)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class UnknownExistingModel {

    // As best practice, when we deprecate or EOL a model,
    // we should mark them in the configuration,
    // instead of removing the whole entry as bad practice!
    // The bad practice should only happen in dev before, add this validation to capture, and
    // confirm it does at least not return 500.
    @Test
    @Order(1)
    public void findOneAndUpdate_sortClause() {
      var collection = "collectionWithBadModel";
      var tableWithBadModel =
          """
                CREATE TABLE "%s"."%s" (
                    key frozen<tuple<tinyint, text>> PRIMARY KEY,
                    array_contains set<text>,
                    array_size map<text, int>,
                    doc_json text,
                    exist_keys set<text>,
                    query_bool_values map<text, tinyint>,
                    query_dbl_values map<text, decimal>,
                    query_null_values set<text>,
                    query_text_values map<text, text>,
                    query_timestamp_values map<text, timestamp>,
                    query_vector_value vector<float, 123>,
                    tx_id timeuuid
                ) WITH additional_write_policy = '99p'
                  AND comment = '{"collection":{"name":"%s","schema_version":1,"options":{"vector":{"dimension":123,"metric":"cosine","service":{"provider":"nvidia","modelName":"random"}},"defaultId":{"type":""}}}}';
                """;
      executeCqlStatement(
          SimpleStatement.newInstance(
              tableWithBadModel.formatted(keyspaceName, collection, collection)));
      givenHeadersAndJson("{ \"findOne\": {} } ")
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collection)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("VECTORIZE_SERVICE_TYPE_UNAVAILABLE"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              containsString("unknown model 'random' for service provider 'nvidia'"));
    }
  }

  private String getAllMetrics() {
    return given().when().get("/metrics").then().statusCode(200).extract().asString();
  }

  private List<String> getVectorizeCallDurationMetrics(
      String commandName, String metrics, int expectedLines) {
    List<String> matches =
        metrics
            .lines()
            .filter(
                line ->
                    line.startsWith("vectorize_call_duration_seconds")
                        && !line.startsWith("vectorize_call_duration_seconds_bucket")
                        && !line.contains("quantile")
                        && line.contains("command=\"" + commandName + "\""))
            .toList();
    // Allow -1 to be passed for "ok to not find any lines" which is acceptable starting state
    if (expectedLines >= 0) {
      assertThat(matches)
          .withFailMessage(
              "Expected to find %d vectorize_call_duration_seconds metrics for command '%s', but found %s.",
              expectedLines, commandName, matches.size())
          .hasSize(expectedLines);
    }
    return matches;
  }

  private static double findEmbeddingCountFromMetrics(List<String> metrics) {
    return findCountFromMetrics(
        metrics,
        Arrays.asList(
            "embedding_provider=\"CustomITEmbeddingProvider\"",
            "module=\"sgv2-jsonapi\"",
            "tenant=\"SINGLE-TENANT\""));
  }

  private static double findCountFromMetrics(List<String> metrics, List<String> matches) {
    String countLine =
        metrics.stream()
            .filter(str -> matches.stream().allMatch(str::contains) && str.contains("_count"))
            .findFirst()
            .orElse(null);
    if (countLine == null) {
      return 0;
    }
    String[] parts = countLine.split(" ");
    String numericPart = parts[parts.length - 1]; // Get the last part which should be the number
    return Double.parseDouble(numericPart);
  }

  private static double findEmbeddingSumFromMetrics(List<String> metrics) {
    return findSumFromMetrics(
        metrics,
        Arrays.asList(
            "embedding_provider=\"CustomITEmbeddingProvider\"",
            "module=\"sgv2-jsonapi\"",
            "tenant=\"SINGLE-TENANT\""));
  }

  private static double findSumFromMetrics(List<String> metrics, List<String> matches) {
    String countLine =
        metrics.stream()
            .filter(str -> matches.stream().allMatch(str::contains) && str.contains("_sum"))
            .findFirst()
            .orElse(null);
    if (countLine == null) {
      return 0;
    }
    String[] parts = countLine.split(" ");
    String numericPart = parts[parts.length - 1]; // Get the last part which should be the number
    return Double.parseDouble(numericPart);
  }
}
