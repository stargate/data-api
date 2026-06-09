package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.api.v1.ResponseAssertions.responseIsStatusOnly;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.response.ValidatableResponse;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests countDocuments with more documents than fit in one page of keys, so the count has to read
 * every page of the result set and not just the first one. DseTestResource is updated to have
 * maxCountLimit above the page size so the count is not capped before a page boundary is crossed.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = CountPaginationIntegrationTest.CountPaginationTestResource.class,
    restrictToAnnotatedClass = true)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CountPaginationIntegrationTest extends AbstractCollectionIntegrationTestBase {

  // One full page of keys plus part of a second page
  private static final int DOCUMENT_COUNT = OperationsConfig.DEFAULT_PAGE_SIZE + 10;
  private static final int MATCHING_DOCUMENT_COUNT = OperationsConfig.DEFAULT_PAGE_SIZE + 2;
  private static final int MAX_COUNT_LIMIT = OperationsConfig.DEFAULT_PAGE_SIZE + 5;

  // Need max count limit above the page size, so counting reads past the first page of keys
  public static class CountPaginationTestResource extends DseTestResource {
    public CountPaginationTestResource() {}

    @Override
    public int getMaxCountLimit() {
      return MAX_COUNT_LIMIT;
    }
  }

  @Test
  @Order(1)
  public void setUp() {
    createKeyspace();
    createSimpleCollection();

    for (int docId = 1; docId <= DOCUMENT_COUNT; docId++) {
      insertDoc(
              """
              {
                "_id": "doc%d",
                "username": "user%d",
                "active_user": %b
              }
              """
              .formatted(docId, docId, docId <= MATCHING_DOCUMENT_COUNT));
    }
  }

  @Test
  public void countSpanningPages() {
    verifyCountCommand(
        MATCHING_DOCUMENT_COUNT,
        """
            {
              "countDocuments": {
                "filter" : {"active_user" : true}
              }
            }
            """);
  }

  @Test
  public void countWithMoreDataSpanningPages() {
    verifyCountCommand(
            MAX_COUNT_LIMIT,
            """
                {
                  "countDocuments": {
                  }
                }
                """)
        .body("status.moreData", is(true));
  }

  protected ValidatableResponse verifyCountCommand(int expectedCount, String json) {
    return givenHeadersPostJsonThenOkNoErrors(json)
        .body("$", responseIsStatusOnly())
        .body("status.count", is(expectedCount));
  }
}
