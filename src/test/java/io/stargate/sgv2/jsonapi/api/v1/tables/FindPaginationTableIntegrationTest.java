package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.scenarios.TestDataScenario.fieldName;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.PartitionedKeyValueTableScenario;
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
  private static final PartitionedKeyValueTableScenario SCENARIO =
      new PartitionedKeyValueTableScenario(keyspaceName, TABLE_NAME);

  private static final Map<String, Object> FILTER_ID =
      ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), "partition-1");

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  /**
   * This test goes through all the pages in each partition and verifies the content. It also checks
   * no sort clause.
   *
   * @throws Exception
   */
  @Test
  public void findWithNoSortAndPartitionedPageState() throws Exception {
    // in total 3 partitions, iterate through each one
    for (int partition = 0; partition < 3; partition++) {
      String partitionedKeyValue = "partition-" + partition;
      Map<String, Object> filter =
          ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), partitionedKeyValue);

      String pageState = null;

      // each partition should have 3 pages
      for (int page = 0; page < 3; page++) {
        Map<String, Object> options =
            pageState == null ? ImmutableMap.of() : ImmutableMap.of("pageState", pageState);

        // send the command and get the result
        var result =
            assertTableCommand(keyspaceName, TABLE_NAME)
                .templated()
                .find(filter, List.of(), null, options) // find all rows in each partition
                .wasSuccessful()
                .hasDocuments(
                    page < 2 ? 20 : 10) // verify the number of documents that are returned
                .verifyDataDocuments(
                    SCENARIO.getPageContent(
                        page * 20, 20, partitionedKeyValue)); // verify the page content;

        if (page < 2) {
          // The first two pages should have next page state, extract it and use it for the next
          // command
          pageState = result.hasNextPageState().extractNextPageState();
        } else {
          // The last page should not have next page state
          result.doesNotHaveNextPageState();
        }
      }
    }
  }

  /**
   * This test checks the empty sort (equals no sort clause). The return data has been verified in
   * the previous test, so this test only checks the page
   */
  @Test
  public void findWithEmptySortAndPageState() {
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
  public void findWithCQLSortAndPageState() {
    // find command with cql sort and extract page state
    Map<String, Object> sort =
        ImmutableMap.of(fieldName(PartitionedKeyValueTableScenario.CLUSTER_COL_1), -1);

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
  public void findWithInMemorySortAndPageState() {
    // find command with in memory sort and it should not have page state
    Map<String, Object> sort =
        ImmutableMap.of(fieldName(PartitionedKeyValueTableScenario.VALUE_COL), 1);

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
