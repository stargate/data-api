package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableDeleteIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String TABLE_WITH_COMPLEX_PRIMARY_KEY = "table_" + System.currentTimeMillis();

  static final String TABLE_DEFINITION_TEMPLATE =
      """
                          {
                              "name": "%s",
                              "definition": {
                                "columns": {
                                  "partition-key-1": "text",
                                  "partition-key-2": "text",
                                  "clustering-key-1": "text",
                                  "clustering-key-2": "text",
                                  "clustering-key-3": "text",
                                  "indexed_column": "text",
                                  "not_indexed_column": "text"
                                },
                                "primaryKey": {
                                  "partitionBy": [
                                    "partition-key-1","partition-key-2"
                                  ],
                                  "partitionSort" : {
                                    "clustering-key-1" : 1, "clustering-key-2" : 1, "clustering-key-3": 1
                                  }
                                }
                              }
                            }
                          """;

  private static Command.CommandName toCommandName(
      WhereCQLClauseAnalyzer.StatementType statementType) {
    return switch (statementType) {
      case DELETE_ONE -> Command.CommandName.DELETE_ONE;
      case DELETE_MANY -> Command.CommandName.DELETE_MANY;
      default -> throw new IllegalArgumentException("Unexpected statement type: " + statementType);
    };
  }

  @BeforeAll
  public final void createTable() {

    assertNamespaceCommand(keyspaceName)
        .postCreateTable(TABLE_DEFINITION_TEMPLATE.formatted(TABLE_WITH_COMPLEX_PRIMARY_KEY))
        .wasSuccessful();

    // Index the column "indexed_column"
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .createIndex(
            "IX_%s_%s".formatted(TABLE_WITH_COMPLEX_PRIMARY_KEY, "indexed_column"),
            "indexed_column")
        .wasSuccessful();
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  private static Stream<Arguments> emptyFilterTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE,
            0));
  }

  @ParameterizedTest
  @MethodSource("emptyFilterTests")
  public void emptyFilter(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                    {}
                    """;

    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> eqAllPrimaryKeysTests() {
    return Stream.of(
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_ONE, null, 1),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_MANY, null, 1));
  }

  @ParameterizedTest
  @MethodSource("eqAllPrimaryKeysTests")
  public void eqAllPrimaryKeys(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                     {
                         "partition-key-1": "partition-key-1-value-default",
                         "partition-key-2": "partition-key-2-value-default",
                         "clustering-key-1": "clustering-key-1-value-default",
                         "clustering-key-2": "clustering-key-2-value-default",
                         "clustering-key-3": "clustering-key-3-value-default"
                     }
                    """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .wasSuccessful()
        .hasNoWarnings();
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  // ==================================================================================================================
  // NON PK COLUMNS - INDEXED AND UN-INDEXED
  // ==================================================================================================================

  private static Stream<Arguments> eqOnNonPrimaryKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE,
            0));
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyTests")
  public void eqOnNonPrimaryKeyIndexed(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
             {
                      "indexed_column": "testData"
                  }
            """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  @ParameterizedTest
  @MethodSource("eqOnNonPrimaryKeyTests")
  public void eqOnNonPrimaryKeyNotIndexed(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
               {
                      "not_indexed_column": "testData"
                  }
            """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  // ==================================================================================================================
  // PARTITION KEY - PARTIAL PARTITION KEY
  // ==================================================================================================================
  private static Stream<Arguments> eqMissingPartitionKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER,
            0));
  }

  @ParameterizedTest
  @MethodSource("eqMissingPartitionKeyTests")
  public void eqMissingPartitionKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                    {
                         "partition-key-1": "partition-key-1-value-default",
                         "clustering-key-1": "clustering-key-1-value-default",
                         "clustering-key-2": "clustering-key-2-value-default",
                         "clustering-key-3": "clustering-key-3-value-default"
                     }
            """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  // ==================================================================================================================
  // CLUSTERING COLUMNS - FULL PARTITION KEY, PARTIAL CLUSTERING KEY
  // Notice, DeleteOne requires FULL PRIMARY KEY columns, DeleteMany only requires all FULL
  // PARTITION KEY and PARTIAL CLUSTERING KEY(Not skipping).
  // ==================================================================================================================

  private static Stream<Arguments> skip1of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER,
            0));
  }

  @ParameterizedTest
  @MethodSource("skip1of3ClusteringKeyTests")
  public void skip1of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                         "partition-key-1": "partition-key-1-value-default",
                         "partition-key-2": "partition-key-2-value-default",
                         "clustering-key-2": "clustering-key-2-value-default",
                         "clustering-key-3": "clustering-key-3-value-default"
                     }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> skip2of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER,
            0));
  }

  @ParameterizedTest
  @MethodSource("skip2of3ClusteringKeyTests")
  public void skip2of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                          "partition-key-1": "partition-key-1-value-default",
                          "partition-key-2": "partition-key-2-value-default",
                          "clustering-key-1": "clustering-key-1-value-default",
                          "clustering-key-3": "clustering-key-3-value-default"
                  }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> skip3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_MANY, null, 2));
  }

  @ParameterizedTest
  @MethodSource("skip3of3ClusteringKeyTests")
  public void skip3of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                    "partition-key-1": "partition-key-1-value-default",
                          "partition-key-2": "partition-key-2-value-default",
                          "clustering-key-1": "clustering-key-1-value-default",
                          "clustering-key-2": "clustering-key-2-value-default"
                  }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> skip1and2of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER,
            0));
  }

  @ParameterizedTest
  @MethodSource("skip1and2of3ClusteringKeyTests")
  public void skip1and2of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                         "partition-key-1": "partition-key-1-value-default",
                          "partition-key-2": "partition-key-2-value-default",
                          "clustering-key-3": "clustering-key-3-value-default"
                  }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> skip2and3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(WhereCQLClauseAnalyzer.StatementType.DELETE_MANY, null, 2));
  }

  @ParameterizedTest
  @MethodSource("skip2and3of3ClusteringKeyTests")
  public void skip2and3of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                         "partition-key-1": "partition-key-1-value-default",
                          "partition-key-2": "partition-key-2-value-default",
                          "clustering-key-1": "clustering-key-1-value-default"
                  }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  private static Stream<Arguments> skip1and3of3ClusteringKeyTests() {
    return Stream.of(
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
            FilterException.Code.FULL_PRIMARY_KEY_REQUIRED_FOR_UPDATE_DELETE,
            0),
        Arguments.of(
            WhereCQLClauseAnalyzer.StatementType.DELETE_MANY,
            FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER,
            0));
  }

  @ParameterizedTest
  @MethodSource("skip1and3of3ClusteringKeyTests")
  public void skip1and3of3ClusteringKey(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {
    deleteAllDefaultRows();
    insertDefaultRows();
    var filterJSON =
        """
                  {
                         "partition-key-1": "partition-key-1-value-default",
                          "partition-key-2": "partition-key-2-value-default",
                          "clustering-key-2": "clustering-key-2-value-default"
                  }
                """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(statementType), filterJSON)
        .hasSingleApiError(expectedCode, FilterException.class);
    checkDataHasBeenDeleted(statementType, expectedCode, shouldDeleteAmount);
  }

  // ==================================================================================================================
  // Helper methods
  // ==================================================================================================================
  static final String DOC_JSON_TEMPLATE =
      """
                {
                    "partition-key-1": "partition-key-1-value-%s",
                    "partition-key-2": "partition-key-2-value-%s",
                    "clustering-key-1": "clustering-key-1-value-%s",
                    "clustering-key-2": "clustering-key-2-value-%s",
                    "clustering-key-3": "clustering-key-3-value-%s",
                    "indexed_column": "index-column-value",
                    "not_indexed_column": "not-indexed-column-value"
                }
                """;

  static final String DOC_JSON_DEFAULT_ROW_1 =
      """
                    {
                        "partition-key-1": "partition-key-1-value-default",
                        "partition-key-2": "partition-key-2-value-default",
                        "clustering-key-1": "clustering-key-1-value-default",
                        "clustering-key-2": "clustering-key-2-value-default",
                        "clustering-key-3": "clustering-key-3-value-default",
                        "indexed_column": "index-column-value",
                        "not_indexed_column": "not-indexed-column-value"
                    }
                    """;

  static final String DOC_JSON_DEFAULT_ROW_2 =
      """
                    {
                        "partition-key-1": "partition-key-1-value-default",
                        "partition-key-2": "partition-key-2-value-default",
                        "clustering-key-1": "clustering-key-1-value-default",
                        "clustering-key-2": "clustering-key-2-value-default",
                        "clustering-key-3": "clustering-key-3-value-default-1",
                        "indexed_column": "index-column-value",
                        "not_indexed_column": "not-indexed-column-value"
                    }
                    """;

  static final List<String> DEFAULT_ROWS = List.of(DOC_JSON_DEFAULT_ROW_1, DOC_JSON_DEFAULT_ROW_2);

  private void insertDefaultRows() {
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .insertMany(DEFAULT_ROWS)
        .wasSuccessful();
  }

  private void deleteAllDefaultRows() {
    // use a partial primary key for deleteMany, then we can delete row_1 and row_2
    var filterJSON =
        """
                         {
                             "partition-key-1": "partition-key-1-value-default",
                             "partition-key-2": "partition-key-2-value-default",
                             "clustering-key-1": "clustering-key-1-value-default",
                             "clustering-key-2": "clustering-key-2-value-default"
                         }
                        """;
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postCommand(toCommandName(WhereCQLClauseAnalyzer.StatementType.DELETE_MANY), filterJSON)
        .wasSuccessful()
        .hasNoErrors();
  }

  // We can't know how many rows are actually got deleted from deleteOne,deleteMany commands
  // DeleteOne should delete row_1, so we can find row_2
  // DeleteMany should delete bow_1 and row_2
  private void checkDataHasBeenDeleted(
      WhereCQLClauseAnalyzer.StatementType statementType,
      FilterException.Code expectedCode,
      int shouldDeleteAmount) {

    // If there is an exception expected, won't execute DeleteCommand, thus no need to check.
    if (expectedCode != null || shouldDeleteAmount == 0) {
      return;
    }
    // Use the SAI indexed column to get all rows
    final String findAllByIndexedColumn =
        """
                              {
                                    "filter": {
                                        "indexed_column": {"$eq" : "index-column-value"}
                                     }
                              }
                              """;

    if (shouldDeleteAmount == DEFAULT_ROWS.size()) {
      assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
          .postFind(findAllByIndexedColumn)
          .wasSuccessful()
          .hasNoWarnings()
          .hasEmptyDataDocuments();
    }
    if (shouldDeleteAmount < DEFAULT_ROWS.size()) {
      assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
          .postFind(findAllByIndexedColumn)
          .wasSuccessful()
          .hasNoWarnings()
          .hasDocuments(DEFAULT_ROWS.size() - shouldDeleteAmount);
    }
  }
}
