package io.stargate.sgv2.jsonapi.api.v1.tables;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableUpdateIntegrationTest extends AbstractTableIntegrationTestBase {

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

  static final String SET_UPDATE_CLAUSE_TEMPLATE =
      """
                  {
                      "$set": {
                        "indexed_column" : "%s",
                        "not_indexed_column": "%s"
                      }
                  }
            """;

  static final String DOC_JSON_DEFAULT_ROW_TEMPLATE =
      """
                          {
                              "partition-key-1": "partition-key-1-value-default",
                              "partition-key-2": "partition-key-2-value-default",
                              "clustering-key-1": "clustering-key-1-value-default",
                              "clustering-key-2": "clustering-key-2-value-default",
                              "clustering-key-3": "clustering-key-3-value-default",
                              "indexed_column": %s,
                              "not_indexed_column": %s
                          }
                          """;

  static final String FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW =
      """
                     {
                         "partition-key-1": "partition-key-1-value-default",
                         "partition-key-2": "partition-key-2-value-default",
                         "clustering-key-1": "clustering-key-1-value-default",
                         "clustering-key-2": "clustering-key-2-value-default",
                         "clustering-key-3": "clustering-key-3-value-default"
                     }

                    """;

  @BeforeAll
  public final void createTable() {
    var tableDefinition = TABLE_DEFINITION_TEMPLATE.formatted(TABLE_WITH_COMPLEX_PRIMARY_KEY);
    createTable(tableDefinition);
    // Index the column "indexed_column"
    createIndex(TABLE_WITH_COMPLEX_PRIMARY_KEY, "indexed_column");

    insertOneInTable(
        TABLE_WITH_COMPLEX_PRIMARY_KEY,
        DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(
            wrapWithDoubleQuote("index-column-value"),
            wrapWithDoubleQuote("not-indexed-column-value")));
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  @Test
  public void updateManyNotSupportedForTable() {
    var updatedValue = "updated_value" + System.currentTimeMillis();
    var updateClauseJSON = SET_UPDATE_CLAUSE_TEMPLATE.formatted(updatedValue, updatedValue);
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne("{}", updateClauseJSON)
        .mayHasSingleApiError(
            FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE, FilterException.class);
  }

  @Test
  public void emptyFilter() {
    var filterJSON =
        """
                 {}
                """;
    var updateJSON =
        SET_UPDATE_CLAUSE_TEMPLATE.formatted(
            "indexed_column_updated_value", "not_indexed_column_updated_value");

    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(filterJSON, updateJSON)
        .mayHasSingleApiError(
            FilterException.Code.FILTER_REQUIRED_FOR_UPDATE_DELETE, FilterException.class);
  }

  @Test
  public void filterOnNonPrimaryKeyColumn() {
    var filterJSON =
        """
                        {
                          "indexed_column" : "%s",
                          "not_indexed_column": "%s"
                        }
                    """;
    var updateJSON =
        SET_UPDATE_CLAUSE_TEMPLATE.formatted(
            "indexed_column_updated_value", "not_indexed_column_updated_value");

    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(filterJSON, updateJSON)
        .mayHasSingleApiError(
            FilterException.Code.NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE, FilterException.class);
  }

  // ==================================================================================================================
  // FILTER has specified a full, existed PRIMARY KEY
  // 1. if update PRIMARY KEY, exception. Exception UPDATE_UNKNOWN_TABLE_COLUMN.
  // 2. if update a column that is not defined in table. Exception UPDATE_PRIMARY_KEY_COLUMN.
  // 3. if update a column that is defined in table, not PRIMARY KEY, good.
  // ==================================================================================================================

  @Test
  public void updateOnNonPrimaryKeyColumnAndExisted() {
    var updatedValue = "updated_value" + System.currentTimeMillis();
    var updateClauseJSON = SET_UPDATE_CLAUSE_TEMPLATE.formatted(updatedValue, updatedValue);
    var expectedUpdatedRow =
        DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(
            wrapWithDoubleQuote(updatedValue), wrapWithDoubleQuote(updatedValue));
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasNoErrorsNoWarnings();
    checkUpdatedData(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, expectedUpdatedRow);
  }

  @Test
  public void updateOnUnknownColumn() {
    var updateClauseJSON =
        """
                    {
                        "$set": {
                            "unknownColumn" : "invalid update"
                         }
                    }
                """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .mayHasSingleApiError(
            UpdateException.Code.UPDATE_UNKNOWN_TABLE_COLUMN, UpdateException.class);
  }

  @Test
  public void updateOnPartitionKey() {
    var updateClauseJSON =
        """
                  {
                    "$set": {
                        "partition-key-1" : "invalid update"
                        }
                  }
              """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .mayHasSingleApiError(
            UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMN, UpdateException.class);
  }

  @Test
  public void updateOnClusteringKey() {
    var updateClauseJSON =
        """
                {
                    "$set": {
                      "clustering-key-1" : "invalid update"
                    }
                }
            """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .mayHasSingleApiError(
            UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMN, UpdateException.class);
  }

  @Test
  public void updateOnBothPartitionKeyAndClusteringKey() {
    var updateClauseJSON =
        """
                {
                  "$set": {
                      "partition-key-1" : "invalid update",
                      "clustering-key-1" : "invalid update"
                    }
                }
            """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .mayHasSingleApiError(
            UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMN, UpdateException.class);
  }

  // ==================================================================================================================
  // Check other update operators.
  // ==================================================================================================================

  @Test
  public void unsetSuccessful() {
    var expectedUpdatedRow = DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(null, null);
    var updateClauseJSON =
        """
                      {
                        "$unset": {
                              "not_indexed_column": "",
                              "indexed_column": ""
                            }
                      }
                  """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasNoErrorsNoWarnings();
    checkUpdatedData(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, expectedUpdatedRow);
  }

  @Test
  public void unsetPrimaryKeyComponent() {
    var updateClauseJSON =
        """
                      {
                        "$unset": {
                              "partition-key-1" : ""
                            }
                      }
                  """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postUpdateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .mayHasSingleApiError(
            UpdateException.Code.UPDATE_PRIMARY_KEY_COLUMN, UpdateException.class);
  }

  private String wrapWithDoubleQuote(String originString) {
    return "\"" + originString + "\"";
  }

  private void checkUpdatedData(String filter, String expectedUpdatedRow) {
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .postFindByFilter(filter)
        .hasNoErrorsNoWarnings()
        .body("data.documents", hasSize(1))
        .body("data.documents[0]", jsonEquals(expectedUpdatedRow));
  }
}
