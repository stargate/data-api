package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsWriteSuccess;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@QuarkusTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@Disabled("Disabled for CI, requires a test configuration where system.size_estimates is enabled")
public class EstimatedDocumentCountIntegrationTest extends AbstractCollectionIntegrationTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(EstimatedDocumentCountIntegrationTest.class);

  private static final int MAX_ITERATIONS = 200;

  private static final int DOCS_PER_ITERATION = 4;

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @Order(1)
  class Count {

    /**
     * Time to wait for the estimated document count to settle after a truncate or insertMany (based
     * on... observed time needed?)
     */
    public static final int TIME_TO_SETTLE_SECS = 75;

    public static final String JSON_ESTIMATED_COUNT =
        """
          {
            "estimatedDocumentCount": {
            }
          }
          """;
    public static final String JSON_ACTUAL_COUNT =
        """
          {
             "countDocuments": {
             }
          }
          """;
    public static final String INSERT_MANY =
        """
            {
              "insertMany": {
                "documents": [
                  {
                      "username": "user1",
                      "subdoc" : {
                         "id" : "12345"
                      },
                      "array" : [
                          "value1"
                      ]
                  },
                  {
                      "username": "user2",
                      "subdoc" : {
                         "id" : "abc"
                      },
                      "array" : [
                          "value2"
                      ]
                  },
                  {
                      "username": "user3",
                      "tags" : ["tag1", "tag2", "tag1234567890123456789012345", null, 1, true],
                      "nestedArray" : [["tag1", "tag2"], ["tag1234567890123456789012345", null]]
                  },
                  {
                      "username": "user4",
                      "indexedObject" : { "0": "value_0", "1": "value_1" }
                  }
                ],
                "options" : {
                  "ordered" : true
                }
              }
            }
            """;

    @Test
    @Order(1)
    public void insertDocuments() throws InterruptedException {

      // execute the insertMany command in a loop until the response indicates a non-zero count, or
      // we have executed the command 100 times
      int tries = 1;
      while (tries <= MAX_ITERATIONS) {
        insertMany();
        Thread.sleep(10L);

        // get count results every N iterations
        if (tries % 10 == 0) {

          int estimatedCount = getEstimatedCount();
          int actualCount = getActualCount();

          LOG.warn(
              "Iteration: "
                  + tries
                  + ", Docs inserted: "
                  + tries * DOCS_PER_ITERATION
                  + ", Actual count: "
                  + actualCount
                  + ", Estimated count: "
                  + estimatedCount);

          if (estimatedCount > 0) {
            break;
          }
        }

        tries++;
      }

      LOG.info(
          "Stopping insertion after non-zero estimated count, now waiting up to "
              + TIME_TO_SETTLE_SECS
              + " seconds for count to settle");

      for (int i = 0; i < TIME_TO_SETTLE_SECS; ++i) {
        int estimatedCount = getEstimatedCount();
        if (estimatedCount > 0) {
          LOG.info("Final estimated count: " + estimatedCount + " -- test passes");
          return;
        }
        if (i % 10 == 0) {
          LOG.info("Estimated count is still zero, waiting...");
        }
        Thread.sleep(1000L);
      }

      fail("Estimated count is zero after " + TIME_TO_SETTLE_SECS + " seconds of wait.");
    }

    /**
     * Verify that truncating the collection (DeleteMany with no filter) yields a zero estimated
     * document count.
     */
    @Test
    @Order(2)
    public void truncate() throws InterruptedException {
      int estimatedCount = getEstimatedCount();
      LOG.info("Estimated count before truncate: " + estimatedCount);

      String jsonTruncate =
          """
              {
                "deleteMany": {
                }
              }
              """;
      givenHeadersPostJsonThenOk(jsonTruncate)
          .body("$", responseIsStatusOnly())
          .body("status.deletedCount", is(-1))
          .body("status.moreData", is(nullValue()));

      LOG.info(
          "Truncated collection, waiting for estimated count to settle for up to "
              + TIME_TO_SETTLE_SECS
              + " seconds");

      for (int i = 0; i < TIME_TO_SETTLE_SECS; ++i) {
        if (estimatedCount < 1) {
          break;
        }
        if (i % 10 == 0) {
          LOG.info("Estimated count still above (" + estimatedCount + "), waiting...");
        }
        Thread.sleep(1000);
      }
      assertThat(estimatedCount).isLessThan(1);
    }

    private int getActualCount() {
      return givenHeadersPostJsonThenOk(JSON_ACTUAL_COUNT)
          .body("$", responseIsStatusOnly())
          .extract()
          .response()
          .jsonPath()
          .getInt("status.count");
    }

    private int getEstimatedCount() {
      return givenHeadersPostJsonThenOk(JSON_ESTIMATED_COUNT)
          .body("$", responseIsStatusOnly())
          .extract()
          .response()
          .jsonPath()
          .getInt("status.count");
    }

    private void insertMany() {
      givenHeadersPostJsonThenOk(INSERT_MANY).body("$", responseIsWriteSuccess());
    }
  }

  @Nested
  @Order(2)
  class Metrics {
    @Test
    public void checkMetrics() {
      EstimatedDocumentCountIntegrationTest.super.checkMetrics("EstimatedDocumentCountCommand");
    }
  }
}
