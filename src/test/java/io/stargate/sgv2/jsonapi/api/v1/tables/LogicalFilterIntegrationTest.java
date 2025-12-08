package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class LogicalFilterIntegrationTest extends AbstractTableIntegrationTestBase {

  static final String TABLE_WITH_COLUMN_TYPES_INDEXED = "logical_table_indexed";

  static final Map<String, Object> ALL_COLUMNS =
      Map.ofEntries(
          Map.entry("id", Map.of("type", "text")),
          Map.entry("age", Map.of("type", "int")),
          Map.entry("name", Map.of("type", "text")),
          Map.entry("is_active", Map.of("type", "boolean")),
          Map.entry("total_views", Map.of("type", "bigint")));

  static final Map<String, Map<String, String>> COLUMNS_CAN_BE_SAI_INDEXED =
      Map.ofEntries(
          // Map.entry("id", Map.of("type", "text")),
          Map.entry("age", Map.of("type", "int")),
          Map.entry("name", Map.of("type", "text")),
          Map.entry("is_active", Map.of("type", "boolean")),
          Map.entry("total_views", Map.of("type", "bigint")));

  static final String SAMPLE_ROW_JSON_1 =
      """
            {
                "id": "1",
                "age": 25,
                "name": "John Doe",
                "is_active": true,
                "total_views": 1000
            }
            """;
  static final String SAMPLE_ROW_JSON_2 =
      """
            {
                "id": "2",
                "age": 30,
                "name": "Jane Smith",
                "is_active": false,
                "total_views": 2000
            }
            """;
  static final String SAMPLE_ROW_JSON_3 =
      """
            {
                "id": "3",
                "age": 35,
                "name": "Alice Johnson",
                "is_active": true,
                "total_views": 3000
            }
            """;

  @BeforeAll
  public final void createDefaultTables() {
    // create table
    assertNamespaceCommand(keyspaceName)
        .templated()
        .createTable(TABLE_WITH_COLUMN_TYPES_INDEXED, ALL_COLUMNS, "id")
        .wasSuccessful();
    // create index on indexable columns
    for (String columnName : COLUMNS_CAN_BE_SAI_INDEXED.keySet()) {
      assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
          .templated()
          .createIndex(TABLE_WITH_COLUMN_TYPES_INDEXED + "_" + columnName, columnName)
          .wasSuccessful();
    }
    // insert 3 rows
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
        .templated()
        .insertMany(SAMPLE_ROW_JSON_1, SAMPLE_ROW_JSON_2, SAMPLE_ROW_JSON_3)
        .wasSuccessful()
        .hasInsertedIdCount(3);
  }

  @Test
  public void simpleOr() {
    var filter =
        """
        {
              "filter": {
                  "$or": [
                      {"age": {"$eq" : 25}},
                      {"name": "Alice Johnson"}
                  ]
              }
        }
        """;
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
        .postFind(filter)
        .hasDocuments(2)
        .hasDocumentUnknowingPosition(SAMPLE_ROW_JSON_1)
        .hasDocumentUnknowingPosition(SAMPLE_ROW_JSON_3)
        .hasNoWarnings()
        .hasNoErrors();
  }

  @Test
  public void oneLevelNestedAndOr() {
    var filter =
        """
            {
                  "filter": {
                      "is_active": {"$eq" : true},
                      "$or": [
                          {"age": {"$lt" : 26}},
                          {"total_views": {"$gt" : 2999}}
                      ]
                  }
            }
            """;
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
        .postFind(filter)
        .hasDocuments(2)
        .hasDocumentUnknowingPosition(SAMPLE_ROW_JSON_1)
        .hasDocumentUnknowingPosition(SAMPLE_ROW_JSON_3)
        .hasNoWarnings()
        .hasNoErrors();
  }

  @Test
  public void twoLevelNestedAndOr() {
    var filter =
        """
            {
                  "filter": {
                      "is_active": {"$eq" : true},
                      "$or": [
                          {
                            "$and": [
                              {"age": {"$lt" : 26} },
                              {"name": {"$ne" : "John Doe"}}
                            ]
                          },
                          {"total_views": {"$gt" : 2999}}
                      ]
                  }
            }
            """;
    assertTableCommand(keyspaceName, TABLE_WITH_COLUMN_TYPES_INDEXED)
        .postFind(filter)
        .hasDocuments(1)
        .hasDocumentUnknowingPosition(SAMPLE_ROW_JSON_3)
        .hasWarning(
            0,
            WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING,
            "The filter uses $ne (not equals) on columns that, while indexed, are still inefficient to filter on using not equals.")
        .hasNoErrors();
  }
}
