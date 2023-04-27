package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class DeleteOneIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  class DeleteOne {
    @Test
    public void byId() {
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
          .body("errors", is(nullValue()));

      json =
          """
          {
            "deleteOne": {
              "filter" : {"_id" : "doc3"}
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
              "filter" : {"_id" : "doc3"}
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
    public void emptyOptionsAllowed() {
      String json =
          """
          {
            "deleteOne": {
              "filter" : {"_id" : "doc3"},
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
          .body("status.deletedCount", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byColumn() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc4",
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
          .body("errors", is(nullValue()));

      json =
          """
          {
            "deleteOne": {
              "filter" : {"username" : "user4"}
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
              "filter" : {"_id" : "doc4"}
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
    public void noFilter() {
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
          .body("errors", is(nullValue()));

      json =
          """
          {
            "deleteOne": {
               "filter": {}
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
              "filter" : {"_id" : "doc3"}
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
    public void noMatch() {
      String json =
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc5",
                "username": "user5"
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
          .body("errors", is(nullValue()));

      json =
          """
          {
            "deleteOne": {
               "filter" : {"username" : "user12345"}
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
          .body("status.deletedCount", is(0))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does find the document
      json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc5"}
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
          .body("data.document._id", is("doc5"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSort() {
      String document =
          """
            {
              "_id": "doc7",
              "username": "user7",
              "active_user" : true
            }
            """;
      insertDoc(document);

      String document1 =
          """
            {
              "_id": "doc6",
              "username": "user6",
              "active_user" : true
            }
            """;
      insertDoc(document1);

      String json =
          """
            {
              "deleteOne": {
                "filter" : {"active_user" : true},
                "sort" : {"username" : 1}
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
          .body("errors", is(nullValue()));

      // assert state after update
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0));

      // cleanUp
      deleteAllDocuments();
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentDeletes() throws Exception {
      String document = """
          {
            "_id": "concurrent"
          }
          """;
      insertDoc(document);

      // we can hit with more threads, max 1 retry per thread
      int threads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 3);
      CountDownLatch latch = new CountDownLatch(threads);

      String deleteJson =
          """
          {
            "deleteOne": {
              "filter" : {"_id" : "concurrent"}
            }
          }
          """;
      // start all threads
      AtomicInteger reportedDeletions = new AtomicInteger(0);
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    Integer deletedCount =
                        given()
                            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                            .contentType(ContentType.JSON)
                            .body(deleteJson)
                            .when()
                            .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                            .then()
                            .statusCode(200)
                            .body("status.deletedCount", anyOf(is(0), is(1)))
                            .body("errors", is(nullValue()))
                            .extract()
                            .path("status.deletedCount");

                    // add reported deletes
                    reportedDeletions.addAndGet(deletedCount);
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

      // assert reported deletes are exactly one
      assertThat(reportedDeletions.get()).isOne();

      // assert state after all deletes
      String findJson =
          """
          {
            "find": {
              "filter" : {"_id" : "concurrent"}
            }
          }
          """;
      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(findJson)
          .when()
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", is(empty()));
    }
  }
}
