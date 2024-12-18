package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static org.hamcrest.Matchers.is;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import net.minidev.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableFilterIntegrationTest extends AbstractTableIntegrationTestBase {

  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(TableFilterIntegrationTest.class);

  static final String TABLE_WITH_COLUMN_TYPES_INDEXED = "table_indexed";
  static final String TABLE_WITH_COLUMN_TYPES_NOT_INDEXED = "table_not_indexed";

  static final String AF_KEYWORD = "performance implications";
  static final List<String> COMPARISON_API_OPERATORS = List.of("$lt", "$gt", "$lte", "$gte");

  static final Map<String, Object> ALL_COLUMNS =
      Map.ofEntries(
          Map.entry("id", Map.of("type", "text")),
          Map.entry("age", Map.of("type", "int")),
          Map.entry("name", Map.of("type", "text")),
          Map.entry("is_active", Map.of("type", "boolean")),
          Map.entry("total_views", Map.of("type", "bigint")),
          Map.entry("price", Map.of("type", "decimal")),
          Map.entry("rating", Map.of("type", "double")),
          Map.entry("weight", Map.of("type", "float")),
          Map.entry("rank", Map.of("type", "smallint")),
          Map.entry("level", Map.of("type", "tinyint")),
          Map.entry("large_number", Map.of("type", "varint")),
          Map.entry("description_ascii", Map.of("type", "ascii")),
          Map.entry("created_at", Map.of("type", "timestamp")),
          Map.entry("event_duration", Map.of("type", "duration")),
          Map.entry("event_date", Map.of("type", "date")),
          Map.entry("event_time", Map.of("type", "time")),
          Map.entry("user_id", Map.of("type", "uuid")),
          Map.entry("ip_address", Map.of("type", "inet")),

          //          Map.entry("session_id", Map.of("type", "timeuuid")),
          Map.entry("image_data", Map.of("type", "blob")));

  static final Map<String, Map<String, String>> COLUMNS_CAN_BE_SAI_INDEXED =
      Map.ofEntries(
          Map.entry("age", Map.of("type", "int")),
          Map.entry("name", Map.of("type", "text")),
          Map.entry("is_active", Map.of("type", "boolean")),
          Map.entry("total_views", Map.of("type", "bigint")),
          Map.entry("price", Map.of("type", "decimal")),
          Map.entry("rating", Map.of("type", "double")),
          Map.entry("weight", Map.of("type", "float")),
          Map.entry("rank", Map.of("type", "smallint")),
          Map.entry("level", Map.of("type", "tinyint")),
          Map.entry("large_number", Map.of("type", "varint")),
          Map.entry("description_ascii", Map.of("type", "ascii")),
          Map.entry("created_at", Map.of("type", "timestamp")),
          Map.entry("event_date", Map.of("type", "date")),
          Map.entry("event_time", Map.of("type", "time")),
          Map.entry("user_id", Map.of("type", "uuid")),
          //          Map.entry("session_id", Map.of("type", "timeuuid")),
          Map.entry("ip_address", Map.of("type", "inet")));

  static final Map<String, Object> COLUMNS_CAN_NOT_BE_INDEXED =
      Map.ofEntries(
          // message=Cannot create secondary index on the only partition key column id,
          Map.entry("id", Map.of("type", "text")),
          // Unsupported type for SAI: blob
          Map.entry("image_data", Map.of("type", "blob")),
          // Secondary indexes are not supported on duration columns
          Map.entry("event_duration", Map.of("type", "duration")));

  final Map<String, List<Object>> SAMPLE_COLUMN_VALUES =
      Map.ofEntries(
          //                  Map.entry("text", List.of("id", quote(SAMPLE_ID))),
          Map.entry("int", List.of("age", 25)),
          Map.entry("bigint", List.of("total_views", 1000000)),
          Map.entry("decimal", List.of("price", 19.99)),
          Map.entry("double", List.of("rating", 4.5d)),
          Map.entry("float", List.of("weight", 70.5f)),
          Map.entry("smallint", List.of("rank", 3)),
          Map.entry("tinyint", List.of("level", 1)),
          Map.entry("varint", List.of("large_number", 123456789)),
          Map.entry("text", List.of("name", "John Doe")),
          Map.entry("boolean", List.of("is_active", true)),
          Map.entry("ascii", List.of("description_ascii", "Sample ASCII Text")),
          // base64 encoded string for "hello world"
          // TODO mayAddQuote may fail on this blob
          Map.entry("blob", List.of("image_data", Map.entry("$binary", "aGVsbG8gd29ybGQ="))),
          Map.entry("date", List.of("event_date", "2024-09-24")),
          Map.entry("time", List.of("event_time", "12:45:01.005")),
          Map.entry("duration", List.of("event_duration", "2h45m")),
          Map.entry("timestamp", List.of("created_at", "2024-09-24T14:06:59Z")),
          Map.entry("uuid", List.of("user_id", "a4233418-ebb5-4453-b681-a02195c7099c")),
          //          Map.entry("timeuuid", List.of("session_id",
          // "a4233418-ebb5-4453-b681-a02195c7099c")),
          Map.entry("inet", List.of("ip_address", "127.0.0.1")));

  static final String SAMPLE_ID = "sampleId";

  final String sampleRowJson =
      constructJsonFromSample(SAMPLE_ID, SAMPLE_COLUMN_VALUES).toJSONString();

  @BeforeAll
  public final void createDefaultTables() {
    // create table and index all columns.
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(TABLE_WITH_COLUMN_TYPES_INDEXED, ALL_COLUMNS, "id")
        .wasSuccessful();

    for (String columnName : COLUMNS_CAN_BE_SAI_INDEXED.keySet()) {
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .templated()
          .createIndex(TABLE_WITH_COLUMN_TYPES_INDEXED + "_" + columnName, columnName)
          .wasSuccessful();
    }

    // create table but no SAI index.
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(TABLE_WITH_COLUMN_TYPES_NOT_INDEXED, ALL_COLUMNS, "id")
        .wasSuccessful();

    // insert 3 sample row
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
        .templated()
        .insertMany(sampleRowJson)
        .wasSuccessful()
        .hasInsertedIdCount(1);

    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_NOT_INDEXED)
        .templated()
        .insertMany(sampleRowJson)
        .wasSuccessful()
        .hasInsertedIdCount(1);
  }

  // $eq
  final String apiFilter$eqTemplate =
      """
                            {
                                  "filter": {
                                      "%s": {"$eq" : %s}
                                   }
                            }
                            """;

  // $ne
  final String apiFilter$neTemplate =
      """
                                {
                                      "filter": {
                                          "%s": {"$ne" : %s}
                                       }
                                }
                                """;

  // $lt,
  final String apiFilter$comparisonTemplate =
      """
                                {
                                      "filter": {
                                          "%s": {"%s" : %s}
                                       }
                                }
                                """;

  // $in
  final String apiFilter$inTemplate =
      """
                                {
                                        "filter": {
                                            "%s": {
                                                "$in": [
                                                    %s,
                                                    %s
                                                ]
                                            }
                                    }
                                }
                                """;

  // $nin
  final String apiFilter$ninTemplate =
      """
                                    {
                                            "filter": {
                                                "%s": {
                                                    "$nin": [
                                                        %s,
                                                        %s
                                                    ]
                                                }
                                        }
                                    }
                                    """;

  @Nested
  public class ScalarColumn {

    private static Stream<Arguments> EQ_ON_SCALAR_COLUMN() {
      return Stream.of(
          Arguments.of("bigint", null, null, SAMPLE_ID),
          Arguments.of("decimal", null, null, SAMPLE_ID),
          Arguments.of("double", null, null, SAMPLE_ID),
          Arguments.of("float", null, null, SAMPLE_ID),
          Arguments.of("smallint", null, null, SAMPLE_ID),
          Arguments.of("tinyint", null, null, SAMPLE_ID),
          Arguments.of("varint", null, null, SAMPLE_ID),
          Arguments.of("text", null, null, SAMPLE_ID),
          Arguments.of("boolean", null, null, SAMPLE_ID),
          Arguments.of("ascii", null, null, SAMPLE_ID),
          Arguments.of("date", null, null, SAMPLE_ID),
          Arguments.of("time", null, null, SAMPLE_ID),
          Arguments.of("timestamp", null, null, SAMPLE_ID),
          Arguments.of("uuid", null, null, SAMPLE_ID),
          //          Arguments.of("timeuuid", null, null, SAMPLE_ID),
          //          Arguments.of("blob", null, null, SAMPLE_ID),
          Arguments.of("inet", null, null, SAMPLE_ID),
          Arguments.of("duration", null, WarningException.Code.MISSING_INDEX, SAMPLE_ID));
    }

    @Test
    public void invalidFilterValueEq() {
      var filter =
              """
                {
                      "filter": {
                          "age": {"$eq" : %s}
                       }
                }
                """
              .formatted("\"invalidType\"");
      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(
              FilterException.Code.INVALID_FILTER_COLUMN_VALUES, FilterException.class);
    }

    @Test
    public void invalidFilterValueIn() {
      var filter =
              """
                {
                      "filter": {
                          "age": {"$in" : [ 123 , %s] }
                       }
                }
                """
              .formatted("\"invalidType\"");
      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(
              FilterException.Code.INVALID_FILTER_COLUMN_VALUES, FilterException.class);
    }

    @ParameterizedTest
    @MethodSource("EQ_ON_SCALAR_COLUMN")
    public void eq(
        String columnType,
        FilterException.Code expectedFilterException,
        WarningException.Code expectedWarningException,
        String expectedDocId) {

      var filter = generateFilter(columnType, apiFilter$eqTemplate);

      // Target column has no index.
      checkFilterOnNoIndexColumn(filter, expectedFilterException, expectedDocId);

      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(expectedFilterException, FilterException.class)
          .mayHasSingleWarning(expectedWarningException)
          .mayFoundSingleDocumentIdByFindOne(expectedFilterException, expectedDocId);
    }

    private static Stream<Arguments> NOT_EQ_ON_SCALAR_COLUMN() {
      return Stream.of(
          Arguments.of("bigint", null, null, null),
          Arguments.of("decimal", null, null, null),
          Arguments.of("double", null, null, null),
          Arguments.of("float", null, null, null),
          Arguments.of("smallint", null, null, null),
          Arguments.of("tinyint", null, null, null),
          Arguments.of("varint", null, null, null),
          Arguments.of(
              "text", null, WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of(
              "boolean", null, WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of(
              "ascii", null, WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of("date", null, null, null),
          Arguments.of("time", null, null, null),
          Arguments.of("timestamp", null, null, null),
          Arguments.of(
              "uuid", null, WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING, null),
          //          Arguments.of("timeuuid", null, null, SAMPLE_ID),
          //          Arguments.of("blob", null, null, SAMPLE_ID),
          Arguments.of("inet", null, null, null),
          Arguments.of("duration", null, WarningException.Code.MISSING_INDEX, null));
    }

    @ParameterizedTest
    @MethodSource("NOT_EQ_ON_SCALAR_COLUMN")
    public void notEq(
        String columnType,
        FilterException.Code expectedFilterException,
        WarningException.Code expectedWarningException,
        String expectedDocId) {

      var filter = generateFilter(columnType, apiFilter$neTemplate);

      // Target column has no index.
      checkFilterOnNoIndexColumn(filter, expectedFilterException, expectedDocId);

      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(expectedFilterException, FilterException.class)
          .mayHasSingleWarning(expectedWarningException)
          .mayFoundSingleDocumentIdByFindOne(expectedFilterException, expectedDocId);
    }

    private static Stream<Arguments> IN_ON_SCALAR_COLUMN() {
      return Stream.of(
          Arguments.of("bigint", null, null, SAMPLE_ID),
          Arguments.of("decimal", null, null, SAMPLE_ID),
          Arguments.of("double", null, null, SAMPLE_ID),
          Arguments.of("float", null, null, SAMPLE_ID),
          Arguments.of("smallint", null, null, SAMPLE_ID),
          Arguments.of("tinyint", null, null, SAMPLE_ID),
          Arguments.of("varint", null, null, SAMPLE_ID),
          Arguments.of("text", null, null, SAMPLE_ID),
          Arguments.of("boolean", null, null, SAMPLE_ID),
          Arguments.of("ascii", null, null, SAMPLE_ID),
          Arguments.of("date", null, null, SAMPLE_ID),
          Arguments.of("time", null, null, SAMPLE_ID),
          Arguments.of("timestamp", null, null, SAMPLE_ID),
          Arguments.of("uuid", null, null, SAMPLE_ID),
          //          Arguments.of("timeuuid", null, null, SAMPLE_ID),
          //          Arguments.of("blob", null, null, SAMPLE_ID),
          Arguments.of("inet", null, null, SAMPLE_ID),
          Arguments.of("duration", null, WarningException.Code.MISSING_INDEX, SAMPLE_ID));
    }

    @ParameterizedTest
    @MethodSource("IN_ON_SCALAR_COLUMN")
    public void in(
        String columnType,
        FilterException.Code expectedFilterException,
        WarningException.Code expectedWarningException,
        String expectedDocId) {

      var filter = generateInFilter(columnType);

      // Target column has no index.
      checkFilterOnNoIndexColumn(filter, expectedFilterException, expectedDocId);

      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(expectedFilterException, FilterException.class)
          .mayHasSingleWarning(expectedWarningException)
          .mayFoundSingleDocumentIdByFindOne(expectedFilterException, expectedDocId);
    }

    private static Stream<Arguments> NOT_IN_ON_SCALAR_COLUMN() {
      return Stream.of(
          Arguments.of("bigint", null, null, null),
          Arguments.of("decimal", null, null, null),
          Arguments.of("double", null, null, null),
          Arguments.of("float", null, null, null),
          Arguments.of("smallint", null, null, null),
          Arguments.of("tinyint", null, null, null),
          Arguments.of("varint", null, null, null),
          Arguments.of("date", null, null, null),
          Arguments.of("time", null, null, null),
          Arguments.of("timestamp", null, null, null),
          //          Arguments.of("timeuuid", null, null, SAMPLE_ID),
          //          Arguments.of("blob", null, null, SAMPLE_ID),
          Arguments.of("duration", null, WarningException.Code.MISSING_INDEX, null),
          Arguments.of("inet", null, null, null),
          Arguments.of(
              "uuid", null, WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of(
              "text", null, WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of(
              "boolean", null, WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING, null),
          Arguments.of(
              "ascii", null, WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING, null));
    }

    @ParameterizedTest
    @MethodSource("NOT_IN_ON_SCALAR_COLUMN")
    public void notIn(
        String columnType,
        FilterException.Code expectedFilterException,
        WarningException.Code expectedWarningException,
        String expectedDocId) {

      var filter = generateNotInFilter(columnType);

      // Target column has no index.
      checkFilterOnNoIndexColumn(filter, expectedFilterException, expectedDocId);

      // Target column has index.
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(expectedFilterException, FilterException.class)
          .mayHasSingleWarning(expectedWarningException)
          .mayFoundSingleDocumentIdByFindOne(expectedFilterException, expectedDocId);
    }

    private static final String NO_DOC_FOUND = "NOTFOUND";

    private static Stream<Arguments> COMPARISON_ON_SCALAR_COLUMN() {
      // ExpectedId in single argument comes as order, >, >=, <, <=
      return Stream.of(
          Arguments.of(
              "bigint",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "decimal",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "double",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "float",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "smallint",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "tinyint",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "varint",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "date",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "time",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "timestamp",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          //          Arguments.of("timeuuid", null, null, Map.of(">",NO_DOC_FOUND,">=",SAMPLE_ID,
          // "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          //          Arguments.of("blob", null, null, SAMPLE_ID),
          Arguments.of(
              "inet",
              null,
              null,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "boolean",
              null,
              WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "text",
              null,
              WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "ascii",
              null,
              WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "uuid",
              null,
              WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING,
              Map.of(">", NO_DOC_FOUND, ">=", SAMPLE_ID, "<", NO_DOC_FOUND, "<=", SAMPLE_ID)),
          Arguments.of(
              "duration",
              FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION,
              null,
              Map.of(
                  ">", NO_DOC_FOUND, ">=", NO_DOC_FOUND, "<", NO_DOC_FOUND, "<=", NO_DOC_FOUND)));
    }

    @ParameterizedTest
    @MethodSource("COMPARISON_ON_SCALAR_COLUMN")
    public void comparison(
        String columnType,
        FilterException.Code expectedFilterException,
        WarningException.Code expectedWarningException,
        Map<String, String> docIdPerComparisonOperator) {

      for (Map.Entry<String, String> comparsionDocEntry : docIdPerComparisonOperator.entrySet()) {
        LOGGER.info(
            "Testing comparison filter %s against column datatype %s"
                .formatted(comparsionDocEntry.getKey(), columnType));

        var expectedDocId =
            comparsionDocEntry.getValue().equals(NO_DOC_FOUND)
                ? null
                : comparsionDocEntry.getValue();
        var filter = generateComparisonFilter(columnType, comparsionDocEntry.getKey());

        // Target column has no index.
        checkFilterOnNoIndexColumn(filter, expectedFilterException, expectedDocId);

        // Target column has index.
        assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
            .postFindOne(filter)
            .mayHaveSingleApiError(expectedFilterException, FilterException.class)
            .mayHasSingleWarning(expectedWarningException)
            .mayFoundSingleDocumentIdByFindOne(expectedFilterException, expectedDocId);
      }
    }
  }

  // ==================================================================================================================
  // Helper methods
  // ==================================================================================================================

  private String generateFilter(String columnType, String filterTemplate) {
    var columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
    var mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));
    return filterTemplate.formatted(columnName, mayAddQuoteValue);
  }

  private String generateInFilter(String columnType) {
    var columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
    var mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));
    return apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue);
  }

  private String generateNotInFilter(String columnType) {
    var columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
    var mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));
    return apiFilter$ninTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue);
  }

  private String generateComparisonFilter(String columnType, String comparisonOperator) {
    var columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
    var mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

    String operator =
        switch (comparisonOperator) {
          case ">" -> "$gt";
          case "<" -> "$lt";
          case "<=" -> "$lte";
          case ">=" -> "$gte";
          default -> throw new IllegalArgumentException("Unexpected value: " + comparisonOperator);
        };

    return apiFilter$comparisonTemplate.formatted(columnName, operator, mayAddQuoteValue);
  }

  private void checkFilterOnNoIndexColumn(
      String filter, FilterException.Code expectedFilterException, String expectedRowId) {
    if (expectedFilterException != null) {
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(filter)
          .mayHaveSingleApiError(expectedFilterException, FilterException.class)
          .hasNoWarnings()
          .hasNoField("data");
      return;
    }
    // If no exception, we then check MISSING_INDEX warning
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_NOT_INDEXED)
        .postFindOne(filter)
        .hasNoErrors()
        .hasSingleWarning(WarningException.Code.MISSING_INDEX)
        .body("data.document.id", is(expectedRowId));
  }

  private String mayAddQuote(Object value) {
    if (value instanceof String) {
      return "\"" + value + "\"";
    }
    return value.toString(); // Return as is for non-string values
  }

  private JSONObject constructJsonFromSample(
      String id, Map<String, List<Object>> sampleColumnValues) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("id", id);
    for (Map.Entry<String, List<Object>> entry : sampleColumnValues.entrySet()) {
      String fieldName = (String) entry.getValue().get(0); // column name
      Object fieldValue = entry.getValue().get(1); // column value
      // Handle blob type specially
      if ("blob".equals(entry.getKey())) {
        jsonObject.put(
            fieldName,
            buildJsonObjectFromMapEntry(
                (Map.Entry<String, String>) fieldValue)); // add the base64 string
      } else {
        jsonObject.put(fieldName, fieldValue);
      }
    }
    return jsonObject;
  }

  private JSONObject buildJsonObjectFromMapEntry(Map.Entry<String, String> entry) {
    JSONObject blobJsonObject = new JSONObject();
    blobJsonObject.put(entry.getKey(), entry.getValue());
    return blobJsonObject;
  }
}
