package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.scenarios.TestDataScenario.ID_COL;
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
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class FindWithClusteringKeyTableIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final String TABLE_NAME = "nonANNSortTableTest";
  private static final ThreeClusteringKeysTableScenario SCENARIO =
      new ThreeClusteringKeysTableScenario(keyspaceName, TABLE_NAME);

  private static Map<String, Object> FILTER_ID = ImmutableMap.of(fieldName(ID_COL), "row-1");

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

  // Tests passing of int not 1 or -1
  @ParameterizedTest
  @MethodSource("findCommandNames")
  public void sortInvalidIntValue(CommandName commandName) {

    Map<String, Object> sort =
        ImmutableMap.of(SCENARIO.fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), 42);

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(commandName, FILTER_ID, null, sort)
        .hasSingleApiError(
            SortException.Code.INVALID_REGULAR_SORT_EXPRESSION,
            SortException.class,
            "The command attempted to use unsupported JSON expression `42` (type Number) for sort clause");
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

  @Test
  public void emptyFilterTriggersImMemorySort() {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), -1);

    var expectedDoc =
        """
            {
                "offset": 19
            }
            """;

    assertTableCommand(keyspaceName, TABLE_NAME)
        .templated()
        .find(Map.of(), PROJECT_OFFSET, sort, ImmutableMap.of("limit", 1))
        .wasSuccessful()
        .hasWarning(
            0,
            WarningException.Code.IN_MEMORY_SORTING_DUE_TO_PARTITION_KEY_NOT_RESTRICTED,
            "When sorting by the partition sorting columns, partition keys needs to restricted by $eq in filter clause")
        .hasWarning(
            1,
            WarningException.Code.ZERO_FILTER_OPERATIONS,
            "Providing zero filters will return all rows in the table, which may have poor performance when the table is large")
        .hasDocuments(1)
        .hasDocumentInPosition(0, expectedDoc);
  }

  private static Stream<Arguments> nonPartitionSelects() {

    var builder = Stream.<Arguments>builder();
    var expectedDoc =
        """
            {
                "offset": 19
            }
            """;
    var expectedDocs = List.of(expectedDoc);

    // all the cases we need the sorting, this is when there is a non partition key in the filter
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$ne", "invalid")),
            expectedDocs,
            true));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$in", new Object[] {"one", "two"})),
            List.of(),
            true));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$gt", "one")), expectedDocs, true));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$gte", "one")),
            expectedDocs,
            true));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$lte", "one")), List.of(), true));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$lt", "one")), List.of(), true));

    // all the cases we don't need the sorting, this is when there is a partition key in the filter
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$eq", "row-1")),
            expectedDocs,
            false));
    builder.add(
        Arguments.of(
            ImmutableMap.of(fieldName(ID_COL), ImmutableMap.of("$in", new Object[] {"row-1"})),
            expectedDocs,
            false));

    return builder.build();
  }

  @ParameterizedTest
  @MethodSource("nonPartitionSelects")
  public void nonPartitionSelectTriggersImMemorySort(
      Map<String, Object> filter, List<String> expectedDocs, boolean inMemorySort) {

    Map<String, Object> sort =
        ImmutableMap.of(fieldName(ThreeClusteringKeysTableScenario.CLUSTER_COL_1), -1);

    var validator =
        assertTableCommand(keyspaceName, TABLE_NAME)
            .templated()
            .find(filter, PROJECT_OFFSET, sort, ImmutableMap.of("limit", 1))
            .wasSuccessful();

    if (inMemorySort) {
      validator.hasWarning(
          0,
          WarningException.Code.IN_MEMORY_SORTING_DUE_TO_PARTITION_KEY_NOT_RESTRICTED,
          "When sorting by the partition sorting columns, partition keys needs to restricted by $eq in filter clause");
    } else {
      validator.hasNoWarnings();
    }

    validator.hasDocuments(expectedDocs.size());
    for (int i = 0; i < expectedDocs.size(); i++) {
      validator.hasDocumentInPosition(i, expectedDocs.get(i));
    }
  }
}
