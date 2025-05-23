package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindOneAndDeleteIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class FindOneAndDelete {
    @Test
    public void byId() {
      final String document =
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """;
      insertDoc(document);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOneAndDelete": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void byIdNoData() {
      insertDoc(
          """
          {
            "_id": "doc3",
            "username": "user3",
            "active_user" : true
          }
          """);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOneAndDelete": {
              "filter" : {"_id" : "doc5"}
            }
          }
          """)
          .body("$", responseIsFindAndSuccess())
          .body("status.deletedCount", is(0));
    }

    @Test
    public void withSortDesc() {
      final String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      final String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : -1}
          }
        }
        """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document))
          .body("status.deletedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSort() {
      final String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      final String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : 1}
          }
        }
        """)
          .body("$", responseIsFindAndSuccess())
          .body("data.document", jsonEquals(document1))
          .body("status.deletedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"_id" : "doc2"}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));
    }

    @Test
    public void withSortProjection() {
      final String document =
          """
        {
          "_id": "doc3",
          "username": "user3",
          "active_user" : true
        }
        """;
      insertDoc(document);

      final String document1 =
          """
        {
          "_id": "doc2",
          "username": "user2",
          "active_user" : true
        }
        """;
      insertDoc(document1);

      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "findOneAndDelete": {
            "filter" : {"active_user" : true},
            "sort" : {"username" : 1},
            "projection" : { "_id":0, "username":1 }
          }
        }
        """)
          .body("$", responseIsFindAndSuccess())
          .body(
              "data.document",
              jsonEquals(
                  """
                {
                  "username": "user2"
                }
                """))
          .body("status.deletedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"_id" : "doc2"}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
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
      FindOneAndDeleteIntegrationTest.checkMetrics("FindOneAndDeleteCommand");
      FindOneAndDeleteIntegrationTest.checkDriverMetricsTenantId();
      FindOneAndDeleteIntegrationTest.checkIndexUsageMetrics("FindOneAndDeleteCommand", false);
    }
  }

  @Nested
  class ConcurrentDelete {

    @Test
    public void findOneAndDelete() throws Exception {
      insertDocuments();

      int threads = 5;
      AtomicReferenceArray<AssertionError> assertionErrors = new AtomicReferenceArray<>(threads);
      CountDownLatch latch = new CountDownLatch(threads);

      for (int i = 0; i < threads; i++) {
        int index = i;
        Thread thread =
            new Thread(
                () -> {
                  try {
                    givenHeadersPostJsonThenOkNoErrors(
                            """
          {
              "findOneAndDelete": {
                  "filter" : {"name" : "Logic Layers"}
              }
          }
          """)
                        .body("$", responseIsFindAndSuccess())
                        .body("status.deletedCount", anyOf(is(0), is(1)));
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
      int threads = 5;
      AtomicReferenceArray<AssertionError> assertionErrors = new AtomicReferenceArray<>(threads);
      CountDownLatch latch = new CountDownLatch(threads);

      for (int i = 0; i < threads; i++) {
        int index = i;
        Thread thread =
            new Thread(
                () -> {
                  try {
                    givenHeadersPostJsonThenOkNoErrors(
                            """
                              {
                                  "findOneAndDelete": {
                                      "filter" : {"name" : "Coded Cleats"},
                                      "projection" : {"name" : 1}
                                  }
                              }
                              """)
                        .body("$", responseIsFindAndSuccess())
                        .body("status.deletedCount", anyOf(is(0), is(1)));
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
    givenHeadersPostJsonThenOkNoErrors(
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
                        """)
        .body("$", responseIsWriteSuccess())
        .body("status.insertedIds[0]", not(emptyString()))
        .statusCode(200);
  }
}
