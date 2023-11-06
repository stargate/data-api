package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndDeleteIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class FindOneAndDelete {
    @Test
    public void byId() {
      String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "findOneAndDelete": {
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
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
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
          .body("data.documents", hasSize(0));
    }

    @Test
    public void byIdNoData() {
      String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      String json =
          """
          {
            "findOneAndDelete": {
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
          .body("status.deletedCount", is(0))
          .body("errors", is(nullValue()));
    }

    @Test
    public void withSortDesc() {
      String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : -1}
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
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
      json =
          """
          {
            "find": {
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
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSort() {
      String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      String json =
          """
        {
          "findOneAndDelete": {
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
          .body("data.document", jsonEquals(document1))
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSortProjection() {
      String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      String expected =
          """
                {
                  "username": "user2"
                }
                """;

      String json =
          """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : 1},
            "projection" : { "_id":0, "username":1 }
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
          .body("data.document", jsonEquals(expected))
          .body("status.deletedCount", is(1))
          .body("errors", is(nullValue()));

      // assert state after update
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
          .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
          .then()
          .statusCode(200)
          .body("data.document", is(nullValue()));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      FindOneAndDeleteIntegrationTest.super.checkMetrics("FindOneAndDeleteCommand");
    }
  }

  @Nested
  class ConcurrentDelete {

    @Test
    public void findOneAndDelete() throws Exception {
      insertDocuments();
      String json =
          """
                                            {
                                                "findOneAndDelete": {
                                                    "filter" : {"name" : "Logic Layers"}
                                                }
                                            }
                                            """;

      int threads = 5;
      AtomicReferenceArray<AssertionError> assertionErrors = new AtomicReferenceArray<>(threads);
      CountDownLatch latch = new CountDownLatch(threads);

      for (int i = 0; i < threads; i++) {
        int index = i;
        Thread thread =
            new Thread(
                () -> {
                  try {
                    given()
                        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                        .contentType(ContentType.JSON)
                        .body(json)
                        .when()
                        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                        .then()
                        .statusCode(200)
                        .body("status.deletedCount", anyOf(is(0), is(1)))
                        .body("errors", is(nullValue()));
                  } catch (AssertionError e) {
                    assertionErrors.set(index, e);
                  } finally {
                    latch.countDown();
                  }
                });
        thread.start();
      }
      latch.await();
      int assertionErrorCount = 0;
      for (int i = 0; i < threads; i++) {
        AssertionError assertionError = assertionErrors.get(i);
        if (null != assertionError) {
          assertionErrorCount++;
        }
      }
      assertThat(assertionErrorCount).isEqualTo(0);
    }

    @Test
    public void findOneAndDeleteProjection() throws Exception {
      insertDocuments();
      String json =
          """
                              {
                                  "findOneAndDelete": {
                                      "filter" : {"name" : "Coded Cleats"},
                                      "projection" : {"name" : 1}
                                  }
                              }
                              """;
      int threads = 5;
      AtomicReferenceArray<AssertionError> assertionErrors = new AtomicReferenceArray<>(threads);
      CountDownLatch latch = new CountDownLatch(threads);

      for (int i = 0; i < threads; i++) {
        int index = i;
        Thread thread =
            new Thread(
                () -> {
                  try {
                    given()
                        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
                        .contentType(ContentType.JSON)
                        .body(json)
                        .when()
                        .post(CollectionResource.BASE_PATH, namespaceName, collectionName)
                        .then()
                        .statusCode(200)
                        .body("status.deletedCount", anyOf(is(0), is(1)))
                        .body("errors", is(nullValue()));
                  } catch (AssertionError e) {
                    assertionErrors.set(index, e);
                  } finally {
                    latch.countDown();
                  }
                });
        thread.start();
      }
      latch.await();
      int assertionErrorCount = 0;
      for (int i = 0; i < threads; i++) {
        AssertionError assertionError = assertionErrors.get(i);
        if (null != assertionError) {
          assertionErrorCount++;
        }
      }
      assertThat(assertionErrorCount).isEqualTo(0);
    }
  }

  public void insertDocuments() {
    String json =
        """
                        {
                           "insertMany": {
                              "documents": [
                                {
                                  "_id": "1",
                                  "name": "Coded Cleats",
                                  "description": "ChatGPT integrated sneakers that talk to you"
                                 },
                                 {
                                   "_id": "2",
                                   "name": "Logic Layers",
                                   "description": "An AI quilt to help you sleep forever"
                                 },
                                 {
                                   "_id": "3",
                                   "name": "Logic Layers",
                                   "description": "An AI quilt to help you sleep forever"
                                 },
                                 {
                                   "_id": "4",
                                   "name": "Vision Vector Frame",
                                   "description": "Vision Vector Frame', 'A deep learning display that controls your mood"
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
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }
}
