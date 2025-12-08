package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.*;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
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
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateManyIntegrationTest extends AbstractCollectionIntegrationTestBase {
  @Nested
  @Order(1)
  class UpdateMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        insertDoc(
                """
            {
              "_id": "doc%s",
              "username": "user%s",
              "active_user" : true
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
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert state after update, first changed document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":false
          }
          """));

      // then not changed document
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc2"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"doc2",
            "username":"user2",
            "active_user":true
          }
          """));
    }

    @Test
    public void emptyOptionsAllowed() {
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": false}},
              "options": {}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());
    }

    @Test
    public void byColumn() {
      insert(5);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert all updated
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents.active_user", everyItem(is(false)));
    }

    @Test
    public void limit() {
      insert(20);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("status.nextPageState", not(emptyOrNullString()));

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"active_user": false}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents.active_user", everyItem(is(false)));
    }

    @Test
    public void limitMoreDataFlag() {
      insert(25);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"active_user" : true},
              "update" : {"$set" : {"active_user": false}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("status.nextPageState", not(emptyOrNullString()));

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"active_user": true}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents.active_user", everyItem(is(true)));
    }

    @Test
    public void updatePagination() {
      insert(25);
      String nextPageState =
          givenHeadersPostJsonThenOkNoErrors(
                  """
              {
                "updateMany": {
                  "filter" : {"active_user" : true},
                  "update" : {"$set" : {"new_data": "new_data_value"}}
                }
              }
              """)
              .body("$", responseIsStatusOnly())
              .body("status.matchedCount", is(20))
              .body("status.modifiedCount", is(20))
              .body("status.moreData", is(true))
              .body("status.nextPageState", notNullValue())
              .extract()
              .body()
              .path("status.nextPageState");

      givenHeadersPostJsonThenOkNoErrors(
                  """
              {
                "updateMany": {
                  "filter" : {"active_user" : true},
                  "update" : {"$set" : {"new_data": "new_data_value"}},
                  "options" : {"pageState": "%s"}}
                }
              }
              """
                  .formatted(nextPageState))
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());
      givenHeadersPostJsonThenOkNoErrors(
              """
        {
          "find": {
            "filter" : {"active_user": true}
          }
        }
        """)
          .body("$", responseIsFindSuccess())
          .body("data.documents.new_data", everyItem(is("new_data_value")));
    }

    @Test
    public void upsert() {
      insert(5);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"_id": "doc6"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert upsert
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc6"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"doc6",
            "active_user":false
          }
          """));
    }

    @Test
    public void upsertWithSetOnInsert() {
      insert(2);
      givenHeadersPostJsonThenOkNoErrors(
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
              """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("docX"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      // assert upsert
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "find": {
                  "filter" : {"_id" : "docX"}
                }
              }
              """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
              {
                "_id": "docX",
                "active_user": true
              }
              """));
    }

    @Test
    public void byIdNoChange() {
      insert(2);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": true}}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":true
          }
          """));
    }

    @Test
    public void upsertManyByColumnUpsert() {
      givenHeadersPostJsonThenOkNoErrors(
              """
              {
                "updateMany": {
                  "filter" : {"location" : "my_city"},
                  "update" : {"$set" : {"active_user": false}},
                  "options" : {"upsert" : true}
                }
              }
              """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is(notNullValue()))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0));

      // assert state after update
      givenHeadersPostJsonThenOkNoErrors(
              """
            {
              "find": {
                "filter" : {"location" : "my_city"}
              }
            }
            """)
          .body("$", responseIsFindSuccess())
          .body("data.documents[0]", is(notNullValue()));
    }

    @Test
    public void upsertAddFilterColumn() {
      insert(5);
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "updateMany": {
              "filter" : {"_id": "doc6", "answer" : 42},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
            }
          }
          """)
          .body("$", responseIsStatusOnly())
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("status.nextPageState", nullValue());

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
          .body(
              "data.documents[0]",
              jsonEquals(
                  """
          {
            "_id":"doc6",
            "answer": 42,
            "active_user": false
          }
          """));
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

      // start all threads
      AtomicReferenceArray<Exception> exceptions = new AtomicReferenceArray<>(threads);
      for (int i = 0; i < threads; i++) {
        int index = i;
        new Thread(
                () -> {
                  try {
                    givenHeadersPostJsonThenOkNoErrors(
                            """
          {
            "updateMany": {
              "update" : {
                "$inc" : {"count": 1}
              }
            }
          }
          """)
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
      givenHeadersPostJsonThenOkNoErrors(
              """
          {
            "find": {
            }
          }
          """)
          .body("$", responseIsFindSuccess())
          .body("data.documents.count", everyItem(is(3)));
    }
  }

  @Nested
  @Order(3)
  class ClientErrors {

    @Test
    public void invalidCommand() {
      givenHeadersPostJsonThenOk(
              """
          {
            "updateMany": {
              "filter" : {"something" : "matching"}
            }
          }
          """)
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
