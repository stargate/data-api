package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.scenarios.TestDataScenario.fieldName;

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
      ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), "row-1");

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  @Test
  public void findWithPageState() {
    // `find_all` command to get the next page state
    String nextPage =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(Map.of(), List.of())
            .wasSuccessful()
            .hasNextPageState()
            .extractNextPageState();

    // It works well with the next page state
    Map<String, Object> options = ImmutableMap.of("pageState", nextPage);
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of(), List.of(), null, options)
        .wasSuccessful()
        .hasNextPageState();
  }

  @Test
  public void findWithEmptySortAndPageState() {
    // This test use the empty sort (equals no sort), and it should be the same as the above test
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
        ImmutableMap.of(fieldName(PartitionedKeyValueTable50Scenario.CLUSTER_COL_1), 1);

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
        .hasNextPageState();
  }

  @Test
  public void findWithInMemorySortAndPageState() {
    // find command with in memory sort and it should not have page state
    Map<String, Object> sort =
        ImmutableMap.of(fieldName(PartitionedKeyValueTable50Scenario.VALUE_COL), 1);

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
  }
}
