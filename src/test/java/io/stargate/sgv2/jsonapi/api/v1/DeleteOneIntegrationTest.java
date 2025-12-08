package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class DeleteOneIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class DeleteOne {
    @Test
    public void byId() {
      givenHeadersPostJsonThenOkNoErrors(
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3"
              }
            }
          }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteOne": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // ensure find does not find the document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void emptyOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteOne": {
              "filter" : {"_id" : "doc3"},
              "options": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(0));
    }

    @Test
    public void noOptionsAllowed() {
      givenHeadersPostJsonThenOk(
              """
              {
                "deleteOne": {
                  "filter" : {"_id" : "docWithOptions"},
                  "options": {"setting":"abc"}
                }
              }
              """)
          .body("$", responseIsError())
          .body("errors", hasSize(1))
          .body("errors[0].errorCode", is("COMMAND_ACCEPTS_NO_OPTIONS"))
          .body("errors[0].exceptionClass", is("JsonApiException"))
          .body("errors[0].message", is("Command accepts no options: `DeleteOneCommand`"));
    }

    @Test
    public void byColumn() {
      givenHeadersPostJsonThenOkNoErrors(
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc4",
                "username": "user4"
              }
            }
          }
          """);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteOne": {
              "filter" : {"username" : "user4"}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // ensure find does not find the document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc4"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void noFilter() {
      givenHeadersPostJsonThenOkNoErrors(
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc3",
                "username": "user3"
              }
            }
          }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteOne": {
               "filter": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // ensure find does not find the document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc3"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document", is(nullValue()));
    }

    @Test
    public void noMatch() {
      givenHeadersPostJsonThenOkNoErrors(
          """
          {
            "insertOne": {
              "document": {
                "_id": "doc5",
                "username": "user5"
              }
            }
          }
          """);

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "deleteOne": {
               "filter" : {"username" : "user12345"}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(0));

      // ensure find does find the document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "findOne": {
              "filter" : {"_id" : "doc5"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.document._id", is("doc5"));
    }

    @Test
    public void withSort() {
      insertDoc(
          """
            {
              "_id": "doc7",
              "username": "user7",
              "active_user" : true
            }
            """);
      insertDoc(
          """
            {
              "_id": "doc6",
              "username": "user6",
              "active_user" : true
            }
            """);

      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "deleteOne": {
                "filter" : {"active_user" : true},
                "sort" : {"username" : 1}
              }
            }
            """)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(1));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"_id" : "doc6"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", hasSize(0));

      // cleanUp
      deleteAllDocuments();
    }
  }

  @Nested
  class Concurrency {

    @RepeatedTest(10)
    public void concurrentDeletes() throws Exception {
      insertDoc(
          """
          {
            "_id": "concurrent"
          }
          """);

      // we can hit with more threads, max 1 retry per thread
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
                                "deleteOne": {
                                  "filter" : {"_id" : "concurrent"}
                                }
                              }
                              """)
                            .body("$", responseIsStatusOnly())
                            .body("status.deletedCount", anyOf(is(0), is(1)))
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
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "concurrent"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents", is(empty()));
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      DeleteOneIntegrationTest.super.checkMetrics("DeleteOneCommand");
      DeleteOneIntegrationTest.super.checkDriverMetricsTenantId();
    }
  }
}
