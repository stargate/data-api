package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateManyIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class UpdateMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
            {
              "_id": "doc%s",
              "username": "user%s",
              "active_user" : true
            }
            """;
        insertDoc(json.formatted(i, i));
      }
    }

    @Test
    public void byId() {
      insert(2);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert state after update, first changed document
      String expected =
          """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
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
          .body("data.documents[0]", jsonEquals(expected));

      // then not changed document
      expected =
          """
          {
            "_id":"doc2",
            "username":"user2",
            "active_user":true
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc2"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": false}},
              "options": {}
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
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());
    }

    @Test
    public void byColumn() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert all updated
      json =
          """
          {
            "find": {
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
          .body("data.documents.active_user", everyItem(is(false)));
    }

    @Test
    public void limit() {
      insert(20);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("status.nextPageState", not(isEmptyOrNullString()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": false}
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
          .body("data.documents.active_user", everyItem(is(false)));
    }

    @Test
    public void limitMoreDataFlag() {
      insert(25);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user" : true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("status.nextPageState", not(isEmptyOrNullString()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": true}
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
          .body("data.documents.active_user", everyItem(is(true)));
    }

    @Test
    public void updatePagination() {
      insert(25);
      String json =
          """
              {
                "updateMany": {
                  "filter" : {"active_user" : true},
                  "update" : {"$set" : {"new_data": "new_data_value"}}
                }
              }
              """;
      String nextPageState =
          given()
              .headers(getHeaders())
              .contentType(ContentType.JSON)
              .body(json)
              .when()
              .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
              .then()
              .statusCode(200)
              .body("$", responseIsStatusOnly())
              .body("status.matchedCount", is(20))
              .body("status.modifiedCount", is(20))
              .body("status.moreData", is(true))
              .body("status.nextPageState", notNullValue())
              .extract()
              .body()
              .path("status.nextPageState");

      json =
              """
              {
                "updateMany": {
                  "filter" : {"active_user" : true},
                  "update" : {"$set" : {"new_data": "new_data_value"}},
                  "options" : {"pageState": "%s"}}
                }
              }
              """
              .formatted(nextPageState);
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());
      json =
          """
        {
          "find": {
            "filter" : {"active_user": true}
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
          .body("data.documents.new_data", everyItem(is("new_data_value")));
    }

    @Test
    public void upsert() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id": "doc6"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert upsert
      String expected =
          """
          {
            "_id":"doc6",
            "active_user":false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc6"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void upsertWithSetOnInsert() {
      insert(2);
      String json =
          """
              {
                "updateMany": {
                  "filter" : {"_id": "no-such-doc"},
                  "update" : {
                    "$set" : {"active_user": true},
                    "$setOnInsert" : {"_id": "docX"}
                  },
                  "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("docX"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert upsert
      String expected =
          """
              {
                "_id": "docX",
                "active_user": true
              }
              """;
      json =
          """
              {
                "find": {
                  "filter" : {"_id" : "docX"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void byIdNoChange() {
      insert(2);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": true}}
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
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      String expected =
          """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":true
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }

    @Test
    public void upsertManyByColumnUpsert() {
      String json =
          """
              {
                "updateMany": {
                  "filter" : {"location" : "my_city"},
                  "update" : {"$set" : {"active_user": false}},
                  "options" : {"upsert" : true}
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
          .body("status.upsertedId", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      json =
          """
            {
              "find": {
                "filter" : {"location" : "my_city"}
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
          .body("data.documents[0]", is(notNullValue()));
    }

    @Test
    public void upsertAddFilterColumn() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id": "doc6", "answer" : 42},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert state after update
      String expected =
          """
          {
            "_id":"doc6",
            "answer": 42,
            "active_user": false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc6"}
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
          .body("data.documents[0]", jsonEquals(expected));
    }
  }

  @Nested
  @Order(2)
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentUpdates() throws Exception {
      // with 5 documents
      String document =
          """
          {
            "_id": "concurrent-%s",
            "count": 0
          }
          """;
      for (int i = 0; i < 5; i++) {
        insertDoc(document.formatted(i));
      }

      // three threads ensures no retries exhausted
      int threads = 3;
      CountDownLatch latch = new CountDownLatch(threads);

      // find all documents
      String updateJson =
          """
          {
            "updateMany": {
              "update" : {
                "$inc" : {"count": 1}
              }
            }
          }
          """;
      // start all threads
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    given()
                        .headers(getHeaders())
                        .contentType(ContentType.JSON)
                        .body(updateJson)
                        .when()
                        .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
                        .then()
                        .statusCode(200)
                        .body("$", responseIsStatusOnly())
                        .body("status.matchedCount", is(5))
                        .body("status.modifiedCount", is(5));
                  } catch (Exception e) {

                    // set exception so we can rethrow
                    exceptions.set(index, e);
                  } finally {

                    // count down
                    latch.countDown();
                  }
                })
            .start();
      }

      latch.await();

      // check if there are any exceptions
      // throw first that is seen
      for (int i = 0; i < threads; i++) {
        Exception exception = exceptions.get(i);
        if (null != exception) {
          throw exception;
        }
      }

      // assert state after all updates
      String findJson =
          """
          {
            "find": {
            }
          }
          """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsFindSuccess())
          .body("data.documents.count", everyItem(is(3)));
    }
  }

  @Nested
  @Order(3)
  class ClientErrors {

    @Test
    public void invalidCommand() {
      String updateJson =
          """
          {
            "updateMany": {
              "filter" : {"something" : "matching"}
            }
          }
          """;
      given()
          .headers(getHeaders())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceName, collectionName)
          .then()
          .statusCode(200)
          .body("$", responseIsError())
          .body("errors[0].errorCode", is("COMMAND_FIELD_INVALID"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid: field 'command.updateClause' value `null` not valid. Problem: must not be null."));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(3)
  class Metrics {
    @Test
    public void checkMetrics() {
      UpdateManyIntegrationTest.checkMetrics("UpdateManyCommand");
      UpdateManyIntegrationTest.checkDriverMetricsTenantId();
      UpdateManyIntegrationTest.checkIndexUsageMetrics("UpdateManyCommand", false);
    }
  }
}
