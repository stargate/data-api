package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class DeleteManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  class DeleteMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
            {
              "insertOne": {
                "document": {
                  "_id": "doc%s",
                  "username": "user%s",
                  "status": "active"
                }
              }
            }
            """;

        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json.formatted(i, i))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
            .then()
            .statusCode(200)
            .body("errors", is(nullValue()));
      }
    }

    @Test
    public void deleteManyById() {
      insert(2);
      String json =
          """
          {
            "deleteMany": {
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
          .body("status.deletedCount", is(1))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the document
      json =
          """
          {
            "findOne": {
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
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));

      // but can find does the non-deleted document
      json =
          """
          {
            "findOne": {
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
          .body("data.docs[0]._id", is("doc2"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyByColumn() {
      insert(5);
      String json =
          """
          {
            "deleteMany": {
              "filter" : {"status": "active"}
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
          .body("status.deletedCount", is(5))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the documents
      json =
          """
          {
            "find": {
              "filter" : {"status": "active"}
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
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyNoFilter() {
      insert(20);
      String json =
          """
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(nullValue()))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure find does not find the documents
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
          .body("data.docs", jsonEquals("[]"))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void deleteManyNoFilterMoreDataFlag() {
      insert(25);
      String json =
          """
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
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(true))
          .body("data", is(nullValue()))
          .body("errors", is(nullValue()));

      // ensure only 20 are really deleted
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
          .body("data.docs", hasSize(5))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentDeletes() throws Exception {
      // with 10 docs
      int totalDocuments = 10;

      String document =
          """
          {
             "_id": "concurrent-%s"
           }
           """;
      for (int i = 0; i < totalDocuments; i++) {
        insertDoc(document.formatted(i));
      }

      // we can hit with more threads, max 1 retry per doc per thread
      int threads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 3);
      CountDownLatch latch = new CountDownLatch(threads);

      String deleteJson =
          """
          {
            "deleteMany": {
            }
          }
          """;
      // start all threads
      AtomicInteger reportedDeletions = new AtomicInteger(0);
      for (int i = 0; i < threads; i++) {
        new Thread(
                () -> {
                  try {
                    Integer deletedCount =
                        given()
                            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                            .contentType(ContentType.JSON)
                            .body(deleteJson)
                            .when()
                            .post(
                                CollectionResource.BASE_PATH,
                                keyspaceId.asInternal(),
                                collectionName)
                            .then()
                            .statusCode(200)
                            .body(
                                "status.deletedCount",
                                anyOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(totalDocuments)))
                            .body("errors", is(nullValue()))
                            .extract()
                            .path("status.deletedCount");

                    // add reported deletes
                    reportedDeletions.addAndGet(deletedCount);
                  } finally {

                    // count down
                    latch.countDown();
                  }
                })
            .start();
      }

      latch.await();

      // assert reported deletes are exactly one
      assertThat(reportedDeletions.get()).isEqualTo(totalDocuments);

      // assert state after all deletes
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
          .body("data.docs", is(empty()));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
