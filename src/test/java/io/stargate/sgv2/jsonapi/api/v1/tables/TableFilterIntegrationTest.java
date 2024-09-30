package io.stargate.sgv2.jsonapi.api.v1.tables;

import static org.hamcrest.Matchers.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.List;
import java.util.Map;
import net.minidev.json.JSONObject;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class TableFilterIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String TABLE_WITH_16_COLUMN_TYPES_INDEXED = "table_indexed";
  static final String TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED = "table_not_indexed";

  static final String ALLOW_FILTERING = "ALLOW FILTERING";
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
          Map.entry("image_data", Map.of("type", "blob")),
          Map.entry("created_at", Map.of("type", "timestamp")),
          Map.entry("event_duration", Map.of("type", "duration")),
          Map.entry("event_date", Map.of("type", "date")),
          Map.entry("event_time", Map.of("type", "time")));

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
          Map.entry("event_time", Map.of("type", "time")));

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
          Map.entry("timestamp", List.of("created_at", "2024-09-24T14:06:59Z")));

  final Map<String, List<Object>> SAMPLE_COLUMN_VALUES_LARGER =
      Map.ofEntries(
          //                  Map.entry("text", List.of("id", quote(SAMPLE_ID))),
          Map.entry("int", List.of("age", 26)),
          Map.entry("bigint", List.of("total_views", 1000005)),
          Map.entry("decimal", List.of("price", 29.99)),
          Map.entry("double", List.of("rating", 5.5d)),
          Map.entry("float", List.of("weight", 75.5f)),
          Map.entry("smallint", List.of("rank", 4)),
          Map.entry("tinyint", List.of("level", 2)),
          Map.entry("varint", List.of("large_number", 223456789)),
          Map.entry("text", List.of("name", "Kally Doe")),
          Map.entry("boolean", List.of("is_active", true)),
          Map.entry("ascii", List.of("description_ascii", "With Larger Sample ASCII Text")),
          // base64 encoded string for "hello world"
          // TODO mayAddQuote may fail on this blob
          Map.entry("blob", List.of("image_data", Map.entry("$binary", "aGVsbG8gd29ybGQ="))),
          Map.entry("date", List.of("event_date", "2024-09-25")),
          Map.entry("time", List.of("event_time", "12:50:01.005")),
          Map.entry("duration", List.of("event_duration", "3h45m")),
          Map.entry("timestamp", List.of("created_at", "2024-10-24T14:06:59Z")));

  final Map<String, List<Object>> SAMPLE_COLUMN_VALUES_SMALLER =
      Map.ofEntries(
          //                  Map.entry("text", List.of("id", quote(SAMPLE_ID))),
          Map.entry("int", List.of("age", 22)),
          Map.entry("bigint", List.of("total_views", 20000)),
          Map.entry("decimal", List.of("price", 10.99)),
          Map.entry("double", List.of("rating", 3.5d)),
          Map.entry("float", List.of("weight", 50.5f)),
          Map.entry("smallint", List.of("rank", 2)),
          Map.entry("tinyint", List.of("level", 0)),
          Map.entry("varint", List.of("large_number", 456789)),
          Map.entry("text", List.of("name", "Apple Orange")),
          Map.entry("boolean", List.of("is_active", false)),
          Map.entry("ascii", List.of("description_ascii", "Apple Sample ASCII Text")),
          // base64 encoded string for "hello world"
          // TODO mayAddQuote may fail on this blob
          Map.entry("blob", List.of("image_data", Map.entry("$binary", "aGVsbG8gd29ybGQ="))),
          Map.entry("date", List.of("event_date", "2024-06-24")),
          Map.entry("time", List.of("event_time", "12:30:01.005")),
          Map.entry("duration", List.of("event_duration", "1h45m")),
          Map.entry("timestamp", List.of("created_at", "2024-05-23T14:06:59Z")));

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

  final String SAMPLE_ID = "sampleId";
  final String SAMPLE_ID_LARGER = "sampleIdLarger";
  final String SAMPLE_ID_SMALLER = "sampleIdSmaller";

  final String sampleRowJson =
      constructJsonFromSample(SAMPLE_ID, SAMPLE_COLUMN_VALUES).toJSONString();
  final String sampleRowJsonLarger =
      constructJsonFromSample(SAMPLE_ID_LARGER, SAMPLE_COLUMN_VALUES_LARGER).toJSONString();
  final String sampleRowJsonSmaller =
      constructJsonFromSample(SAMPLE_ID_SMALLER, SAMPLE_COLUMN_VALUES_SMALLER).toJSONString();

  @BeforeAll
  public final void createDefaultTables() {
    // create table and index all columns.
    createTableWithColumns(TABLE_WITH_16_COLUMN_TYPES_INDEXED, ALL_COLUMNS, "id");
    for (String columnName : COLUMNS_CAN_BE_SAI_INDEXED.keySet()) {
      createIndex(TABLE_WITH_16_COLUMN_TYPES_INDEXED, columnName);
    }
    // create table but no SAI index.
    createTableWithColumns(TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED, ALL_COLUMNS, "id");

    // insert 3 sample row
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_INDEXED, sampleRowJson);
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED, sampleRowJson);
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_INDEXED, sampleRowJsonLarger);
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED, sampleRowJsonLarger);
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_INDEXED, sampleRowJsonSmaller);
    insertOneInTable(TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED, sampleRowJsonSmaller);
  }

  @Nested
  @Order(1)
  class ScalarColumns {

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

    @Test
    public void textColumn() {
      String columnType = "text";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // textColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // textColumnSaiNotIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", is(SAMPLE_ID));

      // textColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // textColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // textColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // textColumnSaiNotIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // textColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // textColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void intColumn() {
      String columnType = "int";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // intColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // intColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // intColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // intColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // intColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // intColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // intColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // intColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void floatColumn() {
      String columnType = "float";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // floatColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // floatColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // floatColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // floatColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // floatColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // floatColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // floatColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // floatColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void smallintColumn() {
      String columnType = "smallint";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // smallintColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // smallintColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // smallintColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // smallintColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // smallintColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // smallintColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // smallintColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // smallintColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void doubleColumn() {
      String columnType = "double";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // doubleColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // doubleColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // doubleColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // doubleColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // doubleColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // doubleColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // doubleColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // doubleColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void decimalColumn() {
      String columnType = "decimal";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // decimalColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // decimalColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // decimalColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // decimalColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // decimalColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // decimalColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // decimalColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // decimalColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void tinyintColumn() {
      String columnType = "tinyint";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // tinyintColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // tinyintColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // tinyintColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // tinyintColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // tinyintColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // tinyintColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // tinyintColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // tinyintColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void varintColumn() {
      String columnType = "varint";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // varintColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // varintColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // varintColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // varintColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // varintColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // varintColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // varintColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // varintColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void bigintColumn() {
      String columnType = "bigint";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // bigintColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // bigintColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // bigintColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // bigintColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // bigintColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // bigintColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // bigintColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // bigintColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void booleanColumn() {
      String columnType = "boolean";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // booleanColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // booleanColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // booleanColumnSaiIndexed $lt,$gt,$lte,$gte
      // Note, the comparison operator for boolean is the comparison of true(1)/false(0)
      // true > false, true == true, true <= true
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        if (comparisonApiOperator.equals("$gt")) {
          // Note, nothing is greater than true
          DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
              .postFindOne(
                  apiFilter$comparisonTemplate.formatted(columnName, comparisonApiOperator, "true"))
              .hasNoErrors()
              .hasNoWarnings()
              .hasNoData();
        } else {
          DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
              .postFindOne(
                  apiFilter$comparisonTemplate.formatted(columnName, comparisonApiOperator, "true"))
              .hasNoErrors()
              .hasNoWarnings()
              .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
        }
      }

      // booleanColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        if (comparisonApiOperator.equals("$gt")) {
          // Note, nothing is greater than true
          DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
              .postFindOne(
                  apiFilter$comparisonTemplate.formatted(columnName, comparisonApiOperator, "true"))
              .hasNoErrors()
              .hasSingleApiWarning("ALLOW FILTERING")
              .hasNoData();
        } else {
          DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
              .postFindOne(
                  apiFilter$comparisonTemplate.formatted(columnName, comparisonApiOperator, "true"))
              .hasNoErrors()
              .hasSingleApiWarning("ALLOW FILTERING")
              .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
        }
      }

      // booleanColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // booleanColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // booleanColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // booleanColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void asciiColumn() {
      String columnType = "ascii";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // asciiColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // asciiColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // asciiColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // asciiColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // asciiColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // asciiColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // asciiColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // asciiColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void dateColumn() {
      String columnType = "date";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // dateColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // dateColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // dateColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // dateColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // dateColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // dateColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // dateColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // dateColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void timeColumn() {
      String columnType = "time";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // timeColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // timeColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // timeColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // timeColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // timeColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timeColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timeColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timeColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void timestampColumn() {
      String columnType = "timestamp";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // timestampColumnSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", is(SAMPLE_ID));

      // timestampColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // timestampColumnSaiIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasNoWarnings()
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // dateColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasNoErrors()
            .hasSingleApiWarning("ALLOW FILTERING")
            .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
      }

      // timestampColumnSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timestampColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timestampColumnSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // timestampColumnSaiNotIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning("ALLOW FILTERING")
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

    @Test
    public void durationColumn() {
      String columnType = "duration";
      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
      String mayAddQuoteValue = mayAddQuote(SAMPLE_COLUMN_VALUES.get(columnType).get(1));

      // Note, "Secondary indexes are not supported on duration columns"

      // durationColumnNotSaiIndexed $eq
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", is(SAMPLE_ID));

      // durationColumnSaiNotIndexed $lt,$gt,$lte,$gte
      for (String comparisonApiOperator : COMPARISON_API_OPERATORS) {
        DataApiCommandSenders.assertTableCommand(
                keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
            .postFindOne(
                apiFilter$comparisonTemplate.formatted(
                    columnName, comparisonApiOperator, mayAddQuoteValue))
            .hasSingleApiError(ErrorCodeV1.TABLE_INVALID_FILTER)
            .hasNoWarnings();
      }

      // durationColumnNotSaiIndexed $ne
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
          .postFindOne(apiFilter$neTemplate.formatted(columnName, mayAddQuoteValue))
          .hasNoErrors()
          .hasSingleApiWarning(ALLOW_FILTERING)
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));

      // durationColumnNotSaiIndexed $in
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
          .postFindOne(
              apiFilter$inTemplate.formatted(columnName, mayAddQuoteValue, mayAddQuoteValue))
          .hasNoErrors()
          .hasNoWarnings()
          .body("data.document.id", oneOf(SAMPLE_ID, SAMPLE_ID_LARGER, SAMPLE_ID_SMALLER));
    }

//    @Test
//    public void blobColumn() {
//      String columnType = "blob";
//      String columnName = (String) SAMPLE_COLUMN_VALUES.get(columnType).get(0);
//      // Note, the $binary EJSON format
//      JSONObject mayAddQuoteValue =
//          buildJsonObjectFromMapEntry(
//              (Map.Entry<String, String>) SAMPLE_COLUMN_VALUES.get(columnType).get(1));
//
//      // blobColumnSaiIndexed
//      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_INDEXED)
//          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue.toJSONString()))
//          .hasNoErrors()
//          .hasNoWarnings()
//          .body("data.document.id", is(SAMPLE_ID));
//
//      // blobColumnNotSaiIndexed
//      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_WITH_16_COLUMN_TYPES_NOT_INDEXED)
//          .postFindOne(apiFilter$eqTemplate.formatted(columnName, mayAddQuoteValue.toJSONString()))
//          .hasNoErrors()
//          .hasSingleApiWarning(ALLOW_FILTERING)
//          .body("data.document.id", is(SAMPLE_ID));
//    }
  }

  private JSONObject buildJsonObjectFromMapEntry(Map.Entry<String, String> entry) {
    JSONObject blobJsonObject = new JSONObject();
    blobJsonObject.put(entry.getKey(), entry.getValue());
    return blobJsonObject;
  }
}
