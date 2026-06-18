package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsFindSuccess;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class LwtRetryIntegrationTest extends AbstractCollectionIntegrationTestBase {

  @RepeatedTest(10)
  public void mixedOperations() throws Exception {
    insertDoc(
        """
        {
          "_id": "doc1",
          "count": 0
        }
        """);

    CountDownLatch latch = new CountDownLatch(2);

    // we know that delete must be executed eventually
    // but for update we might see delete before read, before update or after
    new Thread(
            () -> {
              givenHeadersPostJsonThenOkNoErrors(
                      """
                {
                  "updateOne": {
                    "filter": {
                      "_id": "doc1"
                    },
                    "update" : {
                      "$inc": {"count": 1}
                    }
                  }
                }
                """)
                  .body("$", responseIsStatusOnly())
                  .body("status.matchedCount", anyOf(is(0), is(1)))
                  .body("status.modifiedCount", anyOf(is(0), is(1)));

              latch.countDown();
            })
        .start();

    new Thread(
            () -> {
              givenHeadersPostJsonThenOkNoErrors(
                      """
                {
                  "deleteOne": {
                    "filter": {
                      "_id": "doc1"
                    }
                  }
                }
                """)
                  .body("$", responseIsStatusOnly())
                  .body("status.deletedCount", is(1));

              latch.countDown();
            })
        .start();

    latch.await();

    // ensure there's nothing left
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

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
