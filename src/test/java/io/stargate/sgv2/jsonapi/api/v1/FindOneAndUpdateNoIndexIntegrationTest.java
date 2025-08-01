package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndUpdateNoIndexIntegrationTest extends AbstractKeyspaceIntegrationTestBase {
  private static final String collectionName = "no_index_collection";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nested
  @Order(1)
  class CreateCollection {
    @Test
    public void createBaseCollection() {
      givenHeadersPostJsonThenOk(
                  """
                 {
                   "createCollection": {
                     "name": "%s",
                     "options": {
                       "vector": {
                         "size": 2,
                         "function": "cosine"
                       },
                       "indexing" : {
                         "allow" : ["_id", "name", "value", "indexed"]
                       }
                     }
                   }
                 }
                  """
                  .formatted(collectionName))
          .body("$", responseIsDDLSuccess())
          .body("status.ok", is(1));
    }
  }

  @Nested
  @Order(2)
  class FindAndUpdateWithSet {
    @Test
    public void byIdAfterUpdate() {
      givenHeadersAndJson(
              """
                      {
                        "insertOne": {
                          "document": {
                            "_id": "update_doc_after",
                            "name": "Joe",
                            "age": 42,
                            "enabled": false,
                            "$vector" : [ 0.5, -0.25 ]
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess());

      givenHeadersAndJson(
              """
              {
                "findOneAndUpdate": {
                  "filter" : {"_id" : "update_doc_after"},
                  "update" : {
                    "$set" : {
                      "enabled": true,
                      "value": -1
                    }
                  },
                  "options": {
                    "returnDocument" : "after"
                  }
                }
              }
              """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body(
              "data.document",
              jsonEquals(
                  """
                      {
                        "_id": "update_doc_after",
                        "name": "Joe",
                        "age": 42,
                        "enabled": true,
                        "value": -1
                      }
                      """))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    public void byIdBeforeUpdate() {
      final String DOC =
          """
              {
              "_id": "update_doc_before",
                "name": "Bob",
                "age": 77,
                "enabled": true,
                "value": 3
              }
              """;

      givenHeadersAndJson(
                  """
                              {
                                "insertOne": {
                                  "document": %s
                                }
                              }
                              """
                  .formatted(DOC))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess());

      givenHeadersAndJson(
              """
                      {
                        "findOneAndUpdate": {
                          "filter" : {"_id" : "update_doc_before"},
                          "update" : {
                            "$set" : {
                              "enabled": false,
                              "value": 4
                            }
                          },
                          "options": {
                            "returnDocument": "before"
                          }
                        }
                      }
                      """)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(DOC))
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }
  }

  @Nested
  @Order(3)
  @TestClassOrder(ClassOrderer.OrderAnnotation.class)
  class ArraySizeLimit {
    @Test
    @Order(1)
    public void allowNonIndexedBigArray() {
      insertEmptyDoc("array_size_big_noindex_doc");
      final String arrayJson = bigArray(DocumentLimitsConfig.DEFAULT_MAX_ARRAY_LENGTH + 10);

      givenHeadersAndJson(
                  """
                    {
                      "findOneAndUpdate": {
                        "filter" : {"_id" : "array_size_big_noindex_doc"},
                        "update" : {
                          "$set" : {
                            "notIndexed.bigArray": %s
                          }
                        }
                      }
                    }
                    """
                  .formatted(arrayJson))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindAndSuccess())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1));
    }

    @Test
    @Order(2)
    public void failOnIndexedTooBigArray() {
      insertEmptyDoc("array_size_too_big_doc");
      final String arrayJson = bigArray(DocumentLimitsConfig.DEFAULT_MAX_ARRAY_LENGTH + 10);

      givenHeadersAndJson(
                  """
                            {
                              "findOneAndUpdate": {
                                "filter" : {"_id" : "array_size_too_big_doc"},
                                "update" : {
                                  "$set" : {
                                    "indexed.bigArray": %s
                                  }
                                }
                              }
                            }
                            """
                  .formatted(arrayJson))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
          .body(
              "errors[0].message",
              containsString("number of elements an indexable Array (field 'bigArray')"))
          .body("errors[0].message", containsString("exceeds maximum allowed"));
    }

    @Nested
    @Order(4)
    @TestClassOrder(ClassOrderer.OrderAnnotation.class)
    class ObjectSizeLimit {
      @Test
      @Order(1)
      public void allowNonIndexedBigObject() {
        insertEmptyDoc("object_size_big_noindex_doc");
        final String objectJson =
            bigObject(DocumentLimitsConfig.DEFAULT_MAX_OBJECT_PROPERTIES + 10);
        givenHeadersAndJson(
                    """
                                {
                                  "findOneAndUpdate": {
                                    "filter" : {"_id" : "object_size_big_noindex_doc"},
                                    "update" : {
                                      "$set" : {
                                        "notIndexed.bigObject": %s
                                      }
                                    }
                                  }
                                }
                                """
                    .formatted(objectJson))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
            .then()
            .statusCode(200)
            .body("$", responseIsFindAndSuccess())
            .body("status.matchedCount", is(1))
            .body("status.modifiedCount", is(1));
      }

      @Test
      @Order(2)
      public void failOnIndexedTooBigObject() {
        insertEmptyDoc("object_size_too_big_doc");
        final String objectJson =
            bigObject(DocumentLimitsConfig.DEFAULT_MAX_OBJECT_PROPERTIES + 10);
        givenHeadersAndJson(
                    """
                                {
                                  "findOneAndUpdate": {
                                    "filter" : {"_id" : "object_size_too_big_doc"},
                                    "update" : {
                                      "$set" : {
                                        "indexed.bigObject": %s
                                      }
                                    }
                                  }
                                }
                                """
                    .formatted(objectJson))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
            .then()
            .statusCode(200)
            .body("$", responseIsError())
            .body("errors[0].errorCode", is("SHRED_DOC_LIMIT_VIOLATION"))
            .body(
                "errors[0].message",
                containsString("number of properties an indexable Object (field 'bigObject')"))
            .body("errors[0].message", containsString("exceeds maximum allowed"));
      }
    }

    private void insertEmptyDoc(String docId) {
      givenHeadersAndJson(
                  """
                        {
                          "insertOne": {
                            "document": {
                              "_id": "%s"
                            }
                          }
                        }
                        """
                  .formatted(docId))
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsWriteSuccess())
          .body("status.insertedIds", hasSize(1))
          .body("status.insertedIds[0]", is(docId));
    }

    private String bigArray(int elementCount) {
      final ArrayNode array = MAPPER.createArrayNode();
      for (int i = 0; i < elementCount; i++) {
        array.add(i);
      }
      return array.toString();
    }

    private String bigObject(int propertyCount) {
      final ObjectNode ob = MAPPER.createObjectNode();
      for (int i = 0; i < propertyCount; i++) {
        ob.put("prop" + i, i);
      }
      return ob.toString();
    }
  }
}
