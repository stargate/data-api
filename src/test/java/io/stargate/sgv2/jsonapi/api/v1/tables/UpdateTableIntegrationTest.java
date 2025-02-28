package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UpdateTableIntegrationTest extends AbstractTableIntegrationTestBase {

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

    // Insert default row
    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .insertOne(
            DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(
                wrapWithDoubleQuote("index-column-value"),
                wrapWithDoubleQuote("not-indexed-column-value")))
        .wasSuccessful();
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  @Test
  public void emptyFilter() {
    var updatedValue = "updated_value" + System.currentTimeMillis();
    var updateClauseJSON = SET_UPDATE_CLAUSE_TEMPLATE.formatted(updatedValue, updatedValue);
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne("{}", updateClauseJSON)
        .hasSingleApiError(
            FilterException.Code.MISSING_FILTER_FOR_UPDATE_DELETE, FilterException.class)
        .hasNoWarnings();
  }

  @Test
  public void emptyUpdate() {
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, "{}")
        .hasSingleApiError(UpdateException.Code.MISSING_UPDATE_OPERATIONS, UpdateException.class)
        .hasNoWarnings();
  }

  @Test
  public void unsupportedUpdateOperation() {
    // Take $pop as example, currently not supported
    var updateClauseJSON =
        """
                        {
                            "$mul": {
                              "indexed_column" : 1
                            }
                        }
                  """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_UPDATE_OPERATIONS_FOR_TABLE, UpdateException.class)
        .hasNoWarnings();
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
        .templated()
        .updateOne(filterJSON, updateJSON)
        .hasSingleApiError(
            FilterException.Code.UNSUPPORTED_NON_PRIMARY_KEY_FILTER_FOR_UPDATE_DELETE,
            FilterException.class)
        .hasNoWarnings();
  }

  @Test
  public void updateWithDuplicateColumnAssignments() {
    var updateJSON =
        """
                  {
                       "$set": {
                        "indexed_column": "abc"
                      },
                       "$unset": {
                        "indexed_column": "def"
                      }
                  }
             """;
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_OVERLAPPING_UPDATE_OPERATIONS,
            UpdateException.class,
            "Multiple assignments attempted to change the columns: indexed_column.")
        .hasNoWarnings();
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .wasSuccessful()
        .hasNoWarnings();

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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(UpdateException.Code.UNKNOWN_TABLE_COLUMNS, UpdateException.class)
        .hasNoWarnings();
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS, UpdateException.class)
        .hasNoWarnings();
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS, UpdateException.class)
        .hasNoWarnings();
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS, UpdateException.class)
        .hasNoWarnings();
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .wasSuccessful()
        .hasNoWarnings();
    checkUpdatedData(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, removeNullValues(expectedUpdatedRow));
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
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(
            UpdateException.Code.UNSUPPORTED_UPDATE_FOR_PRIMARY_KEY_COLUMNS, UpdateException.class)
        .hasNoWarnings();
  }

  // ==================================================================================================================
  // Update with empty assignments update operation
  // ==================================================================================================================

  private static Stream<Arguments> EMPTY_ASSIGNMENTS() {
    return Stream.of(
        Arguments.of("{\"$set\":{}}", UpdateException.Code.MISSING_UPDATE_OPERATIONS),
        Arguments.of("{\"$unset\":{}}", UpdateException.Code.MISSING_UPDATE_OPERATIONS),
        Arguments.of(
            "{\"$set\":{}, \"$unset\":{}}", UpdateException.Code.MISSING_UPDATE_OPERATIONS),
        Arguments.of("{\"$set\":{\"not_indexed_column\":\"changed\"}, \"$unset\":{}}", null),
        Arguments.of("{\"$unset\":{\"not_indexed_column\":\"changed\"}, \"$set\":{}}", null));
  }

  @ParameterizedTest
  @MethodSource("EMPTY_ASSIGNMENTS")
  public void emptyAssignments(String updateClauseJSON, UpdateException.Code expectedErrorCode) {

    assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        // Notice, will error out if there is not a single one non-empty assignments update
        // operation.
        .mayHaveSingleApiError(expectedErrorCode, UpdateException.class)
        .hasNoWarnings();
  }

  private String wrapWithDoubleQuote(String originString) {
    return "\"" + originString + "\"";
  }

  private void checkUpdatedData(String filter, String expectedUpdatedRow) {
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .find(filter)
        .wasSuccessful()
        .hasNoWarnings()
        .hasDocuments(1)
        .hasDocumentInPosition(0, expectedUpdatedRow);
  }

  // ==================================================================================================================
  // Update value does not match column dataType
  // ==================================================================================================================

  @Test
  public void setWithUnmatchedDataType() {
    var updateClauseJSON =
        """
                          {
                            "$set": {
                                  "indexed_column": 123
                                }
                          }
                      """;
    // "index_column" has text type, so 123 is invalid
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, updateClauseJSON)
        .hasSingleApiError(UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES, UpdateException.class)
        .hasNoWarnings();
  }

  @Test
  public void unsetWithUnmatchedDataType() {
    // set first, to give "indexed_column" a value
    var setJSON =
        """
                          {
                            "$set": {
                                  "indexed_column": "newValue",
                                  "not_indexed_column": "newValue"
                                }
                          }
                      """;
    var expectedUpdatedRow =
        DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(
            wrapWithDoubleQuote("newValue"), wrapWithDoubleQuote("newValue"));
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, setJSON)
        .hasNoErrors()
        .hasNoWarnings();
    checkUpdatedData(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, expectedUpdatedRow);
    // then unset.
    var unsetJSON =
        """
                          {
                            "$unset": {
                                   "indexed_column": 123,
                                   "not_indexed_column": 456
                                }
                          }
                      """;
    // "index_column" has text type, so 123 is invalid, but $unset does not care about the assign
    // value has the correct type or not
    DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_COMPLEX_PRIMARY_KEY)
        .templated()
        .updateOne(FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, unsetJSON)
        .hasNoErrors()
        .hasNoWarnings();
    var expectedUpdatedRowWithNull = DOC_JSON_DEFAULT_ROW_TEMPLATE.formatted(null, null);
    checkUpdatedData(
        FULL_PRIMARY_KEY_FILTER_DEFAULT_ROW, removeNullValues(expectedUpdatedRowWithNull));
  }

  @Nested
  class MapSetListUpdate {

    static final String MAP_SET_LIST_TABLE_NAME = "table_collection_" + System.currentTimeMillis();

    static final String MAP_SET_LIST_TABLE_DEFINITION =
        """
                    {
                        "name": "%s",
                        "definition": {
                          "columns": {
                            "id": "text",
                            "name": "text",
                            "mapTextToText": {
                                    "type": "map",
                                    "keyType": "text",
                                    "valueType": "text"
                            },
                            "mapIntToInt": {
                                    "type": "map",
                                    "keyType": "int",
                                    "valueType": "int"
                            },
                            "setText": {
                                    "type": "set",
                                    "valueType": "text"
                            },
                            "listText": {
                                    "type": "list",
                                    "valueType": "text"
                            }
                          },
                          "primaryKey": {
                            "partitionBy": [
                              "id"
                            ]
                          }
                        }
                      }
                    """;

    static final String DEFAULT_ROW =
        """
                    {
                        "id": "default-row"
                    }
                    """;

    static final String DEFAULT_ROW_JSON =
        """
                        {
                            "id": "default-row",
                            "name": "default-row"
                        }
                        """;

    static final String EXPECTED_DEFAULT_ROW_MAP_SET_LIST_JSON =
        """
                    {
                        "id": "default-row",
                        "name": "default-row",
                        "mapTextToText": {"key1": "value1"},
                        "mapIntToInt": [[1, 1]],
                        "setText": ["value1"],
                        "listText": ["value1"]
                    }
                    """;

    @BeforeAll
    public static void createTable() {
      assertNamespaceCommand(keyspaceName)
          .postCreateTable(MAP_SET_LIST_TABLE_DEFINITION.formatted(MAP_SET_LIST_TABLE_NAME))
          .wasSuccessful();

      assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .insertOne(DEFAULT_ROW_JSON)
          .wasSuccessful();
    }

    @Test
    @Order(1)
    public void setAndUnset() {
      var setJSON =
          """
                          {
                            "$set": {
                                "mapTextToText": {"key1": "value1"},
                                "mapIntToInt": [[1, 1]],
                                "setText": ["value1"],
                                "listText": ["value1"]
                            }
                          }
                      """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, setJSON)
          .hasNoErrors()
          .hasNoWarnings();
      checkUpdatedData(DEFAULT_ROW, EXPECTED_DEFAULT_ROW_MAP_SET_LIST_JSON);
      // then unset.
      unsetAllMapSetList();
    }

    @Test
    @Order(2)
    public void pushSingle() {
      var pushJSON =
          """
                          {
                            "$push": {
                                "mapTextToText": {"key1": "value1"},
                                "mapIntToInt": [1, 1],
                                "setText": "value1",
                                "listText": "value1"
                            }
                          }
                      """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, pushJSON)
          .hasNoErrors()
          .hasNoWarnings();
      checkUpdatedData(DEFAULT_ROW, EXPECTED_DEFAULT_ROW_MAP_SET_LIST_JSON);
      // then unset.
      unsetAllMapSetList();
    }

    @Test
    @Order(3)
    public void pushMultiple() {
      var pushJSON =
          """
                          {
                            "$push": {
                                "mapTextToText": {"$each": [{"key1": "value1"}]},
                                "mapIntToInt": {"$each": [[1, 1]]},
                                "setText": {"$each": ["value1"]},
                                "listText": {"$each": ["value1"]}
                            }
                          }
                      """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, pushJSON)
          .hasNoErrors()
          .hasNoWarnings();
      checkUpdatedData(DEFAULT_ROW, EXPECTED_DEFAULT_ROW_MAP_SET_LIST_JSON);

      // then unset.
      unsetAllMapSetList();
    }

    @Test
    @Order(4)
    public void pullAll() {
      // make sure we have the data to pullAll
      var setJSON =
          """
                          {
                            "$set": {
                                "mapTextToText": {"key1": "value1"},
                                "mapIntToInt": [[1, 1]],
                                "setText": ["value1"],
                                "listText": ["value1"]
                            }
                          }
                          """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, setJSON)
          .hasNoErrors()
          .hasNoWarnings();
      checkUpdatedData(DEFAULT_ROW, EXPECTED_DEFAULT_ROW_MAP_SET_LIST_JSON);
      // then pullAll
      var pushJSON =
          """
                              {
                                "$pullAll": {
                                    "mapTextToText": ["key1"],
                                    "mapIntToInt": [1],
                                    "setText": ["value1"],
                                    "listText": ["value1"]
                                }
                              }
                          """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, pushJSON)
          .hasNoErrors()
          .hasNoWarnings();
      checkUpdatedData(DEFAULT_ROW, DEFAULT_ROW_JSON);

      // then unset.
      unsetAllMapSetList();
    }

    @ParameterizedTest
    @MethodSource("providePushPullAllOperations")
    public void pushOrPullAllOnPrimitive(String operator) {
      var updateJSON =
              """
              {
                "%s": {
                  "name": "newValue"
                }
              }
              """
              .formatted(operator);

      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, updateJSON)
          .hasSingleApiError(
              UpdateException.Code.UNSUPPORTED_UPDATE_OPERATOR, UpdateException.class)
          .hasNoWarnings();
    }

    private static Stream<Arguments> providePushPullAllOperations() {
      return Stream.of(Arguments.of("$push"), Arguments.of("$pullAll"));
    }

    @ParameterizedTest
    @MethodSource("provideUpdateMapSetListWithNull")
    public void collectionUpdateOperations(String updateClauseJSON) {
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, updateClauseJSON)
          .hasSingleApiError(
              UpdateException.Code.INVALID_UPDATE_COLUMN_VALUES,
              UpdateException.class,
              "null",
              "are not allowed in")
          .hasNoWarnings();
    }

    private static Stream<Arguments> provideUpdateMapSetListWithNull() {
      return Stream.of(
          Arguments.of("{\"$set\": {\"mapTextToText\": [[null,\"abc\"]]}}"),
          Arguments.of("{\"$set\": {\"mapIntToInt\": [[123,null]]}}"),
          Arguments.of("{\"$set\": {\"mapTextToText\": {\"123\":null}}}"),
          Arguments.of("{\"$set\": {\"setText\": [null]}}"),
          Arguments.of("{\"$set\": {\"listText\": [null]}}"),
          Arguments.of("{\"$push\": {\"mapTextToText\": [null,\"abc\"]}}"),
          Arguments.of("{\"$push\": {\"mapIntToInt\": [123, null]}}"),
          Arguments.of("{\"$push\": {\"setText\": null}}"),
          Arguments.of("{\"$push\": {\"listText\": null}}"),
          Arguments.of("{\"$pullAll\": {\"mapTextToText\": [null]}}"),
          Arguments.of("{\"$pullAll\": {\"setText\": [null]}}"),
          Arguments.of("{\"$pullAll\": {\"listText\": [null]}}"));
    }

    private void unsetAllMapSetList() {
      var unsetJSON =
          """
                          {
                            "$unset": {
                                  "mapTextToText": {"123": "455"},
                                  "mapIntToInt": [],
                                  "setText": ["random"],
                                  "listText": "abc"
                                }
                          }
                      """;
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .updateOne(DEFAULT_ROW, unsetJSON)
          .hasNoErrors()
          .hasNoWarnings();

      checkUpdatedData(DEFAULT_ROW, DEFAULT_ROW_JSON);
    }

    private void checkUpdatedData(String filter, String expectedUpdatedRow) {
      DataApiCommandSenders.assertTableCommand(keyspaceName, MAP_SET_LIST_TABLE_NAME)
          .templated()
          .find(filter)
          .wasSuccessful()
          .hasNoWarnings()
          .hasDocuments(1)
          .hasDocumentInPosition(0, expectedUpdatedRow);
    }
  }
}
