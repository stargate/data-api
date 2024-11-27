package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.scenarios.TestDataScenario.fieldName;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.PartitionedKeyValueTable50Scenario;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.ThreeClusteringKeysTableScenario;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindPaginationTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "findPaginationTableIntegrationTest";
  private static final PartitionedKeyValueTable50Scenario SCENARIO =
      new PartitionedKeyValueTable50Scenario(keyspaceName, TABLE_NAME);

  private static Map<String, Object> FILTER_ID =
      ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), "partition-1");

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  @Test
  @Order(1)
  public void findWithNoSortAndPartitionedPageState() throws Exception {
    // This test goes through all the pages in each partition and verifies the content
    // It also checks no sort clause
    for (int partition = 0; partition < 3; partition++) {
      String partitionedKeyValue = "partition-" + partition;
      FILTER_ID =
          ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), partitionedKeyValue);
      String pageState =
          assertTableCommand(keyspaceName, TABLE_NAME)
              .templated()
              .find(FILTER_ID, List.of(), null) // find all rows in each partition
              .wasSuccessful()
              .body("data.documents", hasSize(20)) // Verify that 20 rows are returned.
              .body(
                  "data.documents",
                  jsonEquals(
                      SCENARIO.getPageContent(0, 20, partitionedKeyValue))) // verify the content
              .hasNextPageState()
              .extractNextPageState();

      Map<String, Object> options = ImmutableMap.of("pageState", pageState);

      // The next page should have 10 rows and no page state
      assertTableCommand(keyspaceName, TABLE_NAME)
          .templated()
          .find(FILTER_ID, List.of(), null, options)
          .wasSuccessful()
          .body("data.documents", hasSize(10)) // 10 rows for the last page
          .body(
              "data.documents",
              jsonEquals(
                  SCENARIO.getPageContent(20, 20, partitionedKeyValue))) // verify the content
          .doesNotHaveNextPageState();
    }
  }

  @Test
  @Order(2)
  public void findWithEmptySortAndPageState() {
    // This test checks the empty sort (equals no sort clause)
    // The return data has been verified in the previous test, so this test only checks the page
    // state
    String nextPage =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(Map.of(), List.of(), Map.of())
            .wasSuccessful()
            .hasNextPageState()
            .extractNextPageState();

    // It works well with the next page state
    Map<String, Object> options = ImmutableMap.of("pageState", nextPage);
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of(), List.of(), Map.of(), options)
        .wasSuccessful()
        .hasNextPageState();
  }

  @Test
  @Order(3)
  public void findWithCQLSortAndPageState() {
    // find command with cql sort and extract page state
    Map<String, Object> sort =
        ImmutableMap.of(fieldName(PartitionedKeyValueTable50Scenario.CLUSTER_COL_1), -1);

    String nextPage =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(FILTER_ID, List.of(), sort)
            .wasSuccessful()
            .hasNextPageState()
            .extractNextPageState();

    // use the page state from the above without any error
    Map<String, Object> options = ImmutableMap.of("pageState", nextPage);
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, List.of(), sort, options)
        .wasSuccessful()
        .doesNotHaveNextPageState();

    // ok to pass empty page state which will be ignored, and return the same page state
    options = ImmutableMap.of("pageState", "");
    String pageState =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(FILTER_ID, List.of(), sort, options)
            .wasSuccessful()
            .hasNextPageState()
            .extractNextPageState();
    Assertions.assertEquals(
        nextPage,
        pageState,
        "The page state should match when empty string is passed as page state");
  }

  @Test
  @Order(4)
  public void findWithInMemorySortAndPageState() {
    // find command with in memory sort and it should not have page state
    Map<String, Object> sort =
        ImmutableMap.of(fieldName(PartitionedKeyValueTable50Scenario.VALUE_COL), 1);

    // should not have page state returned
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, List.of(), sort)
        .wasSuccessful()
        .doesNotHaveNextPageState();

    // error if page state is passed with in memory sort
    Map<String, Object> options = ImmutableMap.of("pageState", "AAgABAAAAOYBAYDn8H///+s=");
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, List.of(), sort, options)
        .hasSingleApiError(
            SortException.Code.UNSUPPORTED_PAGINATION_WITH_IN_MEMORY_SORTING,
            SortException.class,
            "Pagination is not supported when the data is sorted in-memory");

    // ok to pass empty page state
    options = ImmutableMap.of("pageState", "");
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, List.of(), sort, options)
        .wasSuccessful()
        .doesNotHaveNextPageState();
  }
}
