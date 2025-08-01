package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class DeleteManyIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class DeleteMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        insertDoc(
                """
            {
                  "_id": "doc%s",
                  "username": "user%s",
                  "status": "active"
            }
            """
                .formatted(i, i));
      }
    }

    @Test
    public void byId() {
      insert(2);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1))
          .body("status.moreData", is(nullValue()));

      // ensure find does not find the document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess());

      // but can find does the non-deleted document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc2"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("doc2"));
    }

    @Test
    public void emptyOptionsAllowed() {
      insert(2);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
              "filter" : {"_id" : "doc1"},
              "options": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1))
          .body("status.moreData", is(nullValue()));
    }

    @Test
    public void byColumn() {
      insert(5);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
              "filter" : {"status": "active"}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(5))
          .body("status.moreData", is(nullValue()));

      // ensure find does not find the documents
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"status": "active"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", jsonEquals("[]"));
    }

    /**
     * Verify that DeleteMany with no filter will use truncate and delete all documents in a
     * collection, instead of using Delete with maximum number to delete.
     */
    @Test
    public void noFilter() {
      insert(25);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(-1))
          .body("status.moreData", is(nullValue()));

      // ensure find does not find the documents
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": { }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", jsonEquals("[]"));
    }

    /**
     * Verify that DeleteMany with empty filter will use truncate and delete all documents in a
     * collection, instead of using Delete with maximum number to delete.
     */
    @Test
    public void emptyFilter() {
      insert(25);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
              "filter": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(-1))
          .body("status.moreData", is(nullValue()));

      // ensure find does not find the documents
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": { }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", jsonEquals("[]"));
    }

    @Test
    public void withFilterMoreDataFlag() {
      insert(25);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteMany": {
              "filter" : {"status": "active"}
            }
          }
          """)
          // moreData will only exist when filter exist. If filter doesn't exist, it will delete all
          // data
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(20))
          .body("status.moreData", is(true));

      // ensure only 20 are really deleted
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(5));
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentDeletes() throws Exception {
      // with 10 documents
      int totalDocuments = 10;
      String document =
          """
          {
            "_id": "concurrent-%s",
            "status": "active"
          }
          """;
      for (int i = 0; i < totalDocuments; i++) {
        insertDoc(document.formatted(i));
      }

      // we can hit with more threads, max 1 retry per doc per thread
      int threads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 3);
      CountDownLatch latch = new CountDownLatch(threads);

      // start all threads
      AtomicInteger reportedDeletions = new AtomicInteger(0);
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    Integer deletedCount =
                        givenHeadersPostJsonThenOkNoErrors(
                                """
                            {
                              "deleteMany": {
                                "filter" : {"status": "active"}
                              }
                            }
                            """)
                            .body("$", responseIsStatusOnly())
                            .body(
                                "status.deletedCount",
                                anyOf(greaterThanOrEqualTo(0), lessThanOrEqualTo(totalDocuments)))
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
      assertThat(reportedDeletions.get()).isEqualTo(totalDocuments);

      // assert state after all deletes
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", is(empty()));
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
      DeleteManyIntegrationTest.super.checkMetrics("DeleteManyCommand");
      DeleteManyIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
