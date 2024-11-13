package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.scenarios.TestDataScenario.fieldName;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.v1.util.scenarios.ThreeClusteringKeysTableScenario;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * NOTE: not checking that sorting works, checking that we give out warnings when not doing DB
 * sorting and basic checks that when we do db sorting we get expected results.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class SortByClusteringTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "nonANNSortTableTest";
  private static final ThreeClusteringKeysTableScenario SCENARIO =
      new ThreeClusteringKeysTableScenario(keyspaceName, TABLE_NAME);

  private static Map<String, Object> FILTER_ID =
      ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.ID_COL), "row-1");
  private static List<String> PROJECT_OFFSET =
      List.of(fieldName(ThreeClusteringKeysTableScenario.OFFSET_COL));

  @BeforeAll
  public final void createScenario() {
    SCENARIO.create();
  }

  @AfterAll
  public final void dropScenario() {
    SCENARIO.drop();
  }

  private static Stream<Arguments> findCommandNames() {

    var commands = new ArrayList<Arguments>();
    commands.add(Arguments.of(CommandName.FIND));
    commands.add(Arguments.of(CommandName.FIND_ONE));
    return commands.stream();
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortUnknownColumn(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(
            SCENARIO.fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1) + "unknown", 1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleApiError(
            SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS,
            SortException.class,
            "The command attempted to sort the unknown columns: %s."
                .formatted(
                    SCENARIO.fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1)
                        + "unknown"));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortNonPartition(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.OFFSET_COL), 1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleWarning(
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING,
            "The command sorted on the columns: %s.".formatted(errFmtJoin(sort.keySet())));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortNonPartitionAndPartition(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(
            fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), 1,
            fieldName(ThreeClusteringKeysTableScenario.OFFSET_COL), 1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleWarning(
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_NON_PARTITION_SORTING,
            "The command used columns in the sort clause that are not part of the partition sorting, and so the query was sorted in memory.");
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortLastOnlyClusteringFirst(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_3), 1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleWarning(
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_OUT_OF_ORDER_PARTITION_SORTING,
            "The sort clause used the columns (in order) : %s."
                .formatted(errFmtJoin(sort.keySet())));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortLastThenFirstClusteringFirst(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(
            fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_3),
            1,
            fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1),
            1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleWarning(
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_OUT_OF_ORDER_PARTITION_SORTING,
            "The sort clause used the columns (in order) : %s."
                .formatted(errFmtJoin(sort.keySet())));
  }

  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortFirstThenLastClusteringFirst(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(
            fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1),
            1,
            fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_3),
            1);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleWarning(
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_OUT_OF_ORDER_PARTITION_SORTING,
            "The sort clause used the columns (in order) : %s."
                .formatted(errFmtJoin(sort.keySet())));
  }

  @Test
  public void findOneSort1Asc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), 1);

    var expected =
        """
        {
            "offset": 0
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .findOne(FILTER_ID, PROJECT_OFFSET, sort)
        .wasSuccessful()
        .hasNoWarnings()
        .hasSingleDocument(expected);
  }

  @Test
  public void findOneSort1Desc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), -1);

    var expected =
        """
        {
            "offset": 19
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .findOne(FILTER_ID, PROJECT_OFFSET, sort)
        .wasSuccessful()
        .hasNoWarnings()
        .hasSingleDocument(expected);
  }

  @Test
  public void findManyLimit1Sort1Asc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), 1);

    var expected =
        """
        {
            "offset": 0
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, PROJECT_OFFSET, sort, ImmutableMap.of("limit", 1))
        .wasSuccessful()
        .hasNoWarnings()
        .hasDocuments(1)
        .hasDocumentInPosition(0, expected);
  }

  @Test
  public void findManyLimit1Sort1Desc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), -1);

    var expected =
        """
        {
            "offset": 19
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, PROJECT_OFFSET, sort, ImmutableMap.of("limit", 1))
        .wasSuccessful()
        .hasNoWarnings()
        .hasDocuments(1)
        .hasDocumentInPosition(0, expected);
  }

  @Test
  public void findManyLimit3Sort1Asc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), 1);

    var expected1 =
        """
        {
            "offset": 0
        }
        """;
    var expected2 =
        """
        {
            "offset": 1
        }
        """;
    var expected3 =
        """
        {
            "offset": 2
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, PROJECT_OFFSET, sort, ImmutableMap.of("limit", 3))
        .wasSuccessful()
        .hasNoWarnings()
        .hasDocuments(3)
        .hasDocumentInPosition(0, expected1)
        .hasDocumentInPosition(1, expected2)
        .hasDocumentInPosition(2, expected3);
  }

  @Test
  public void findManyLimit3Sort1Desc() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), -1);

    var expected1 =
        """
        {
            "offset": 19
        }
        """;
    var expected2 =
        """
        {
            "offset": 18
        }
        """;
    var expected3 =
        """
        {
            "offset": 17
        }
        """;
    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(FILTER_ID, PROJECT_OFFSET, sort, ImmutableMap.of("limit", 3))
        .wasSuccessful()
        .hasNoWarnings()
        .hasDocuments(3)
        .hasDocumentInPosition(0, expected1)
        .hasDocumentInPosition(1, expected2)
        .hasDocumentInPosition(2, expected3);
  }
}
