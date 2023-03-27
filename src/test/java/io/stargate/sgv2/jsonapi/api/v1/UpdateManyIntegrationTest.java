package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class UpdateManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
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
    public void updateManyById() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));

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
    public void updateManyByColumn() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      // assert all updated
      json = """
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
          .body("data.docs.active_user", everyItem(is(false)));
    }

    @Test
    public void updateManyLimit() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": false}
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
          .body("data.docs.active_user", everyItem(is(false)))
          .body("data.count", is(20))
          .body("errors", is(nullValue()));
    }

    @Test
    public void updateManyLimitMoreDataFlag() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": true}
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
          .body("data.docs.active_user", everyItem(is(true)))
          .body("data.count", is(5));
    }

    @Test
    public void updateManyUpsert() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

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
    public void updateManyByIdNoChange() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.upsertedId", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("errors", is(nullValue()));

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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", is(notNullValue()));
    }

    @Test
    public void updateManyUpsertAddFilterColumn() {
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentUpdates() throws Exception {
      // with 5 docs
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

      // find all docs
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
                        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                        .contentType(ContentType.JSON)
                        .body(updateJson)
                        .when()
                        .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
                        .then()
                        .statusCode(200)
                        .body("status.matchedCount", is(5))
                        .body("status.modifiedCount", is(5))
                        .body("errors", is(nullValue()));
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.docs.count", everyItem(is(3)))
          .body("data.count", is(5));
    }
  }

  @Nested
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
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(updateJson)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data", is(nullValue()))
          .body("status", is(nullValue()))
          .body("errors[0].exceptionClass", is("ConstraintViolationException"))
          .body(
              "errors[0].message",
              is(
                  "Request invalid, the field postCommand.command.updateClause not valid: must not be null."));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
