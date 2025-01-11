package io.stargate.sgv2.jsonapi.api.v1.tables;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertNamespaceCommand;
import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertTableCommand;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class ListIndexesIntegrationTest extends AbstractTableIntegrationTestBase {
  private static final String TABLE = "person";
  private static final String createIndex =
      """
              {
                "name": "name_idx",
                "definition": {
                  "column": "name",
                  "options": {
                    "ascii": true,
                    "caseSensitive": false,
                    "normalize": true
                  }
                },
                "indexType": "regular"
              }
              """;

  String createWithoutOptions =
      """
                  {
                    "name": "city_idx",
                    "definition": {
                      "column": "city"
                    }
                  }
                  """;

  String createWithoutOptionsExpected =
      """
                  {
                    "name": "city_idx",
                    "definition": {
                      "column": "city",
                      "options": {
                        "ascii": false,
                        "caseSensitive": true,
                        "normalize": false
                      }
                    },
                    "indexType": "regular"
                  }
                  """;

  String createVectorIndex =
      """
                {
                 "name": "content_idx",
                 "definition": {
                   "column": "content",
                   "options": {
                     "metric": "cosine",
                     "sourceModel": "openai-v3-small"
                   }
                 },
                 "indexType": "vector"
               }
              """;

  @BeforeAll
  public final void createDefaultTablesAndIndexes() {
    String tableData =
        """
            {
               "name": "%s",
               "definition": {
                   "columns": {
                       "id": "text",
                       "age": "int",
                       "name": "text",
                       "city": "text",
                       "content": {
                           "type": "vector",
                           "dimension": 1024
                       }
                   },
                   "primaryKey": "id"
               }
           }
        """;
    assertNamespaceCommand(keyspaceName)
        .postCreateTable(tableData.formatted(TABLE))
        .wasSuccessful();
    // index1, name_idx
    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createIndex).wasSuccessful();
    // index2, city_idx
    assertTableCommand(keyspaceName, TABLE).postCreateIndex(createWithoutOptions).wasSuccessful();
    // index3, content_idx
    assertTableCommand(keyspaceName, TABLE)
        .postCreateVectorIndex(createVectorIndex)
        .wasSuccessful();
  }

  @Nested
  @Order(1)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class ListIndexes {

    @Test
    @Order(1)
    public void listIndexesOnly() {

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .listIndexes(false)
          .wasSuccessful()
          .hasIndexes("city_idx", "name_idx", "content_idx");
    }

    @Test
    @Order(2)
    public void listIndexesWithDefinition() {

      assertTableCommand(keyspaceName, TABLE)
          .templated()
          .listIndexes(true)
          .wasSuccessful()
          // Validate that status.indexes has all indexes for the table
          .body("status.indexes", hasSize(3))
          // Validate index without options
          .body(
              "status.indexes",
              containsInAnyOrder( // Validate that the indexes are in any order
                  jsonEquals(createIndex),
                  jsonEquals(createWithoutOptionsExpected),
                  jsonEquals(createVectorIndex)));
    }
  }

  // ==================================================================================================================
  // This subClass is to test some index corner cases.
  // 1. Currently, Data API does not support create index on set/list/map column, so we need to
  // create these indexes in CQL, and then test ListIndexes command
  // 2. Data API resolves index target from IndexMetaData, detail in {@link CQLSAIINDEX}, test
  // columns with doubleQuote
  // ==================================================================================================================
  @Nested
  public class CreatedIndexOnPreExistedCqlTable {
    private static final String PRE_EXISTED_CQL_TABLE = "perExistedCqlTable";

    @Test
    @Order(1)
    public final void createPerExistedCqlTable() {
      // Build the CREATE TABLE statement
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(PRE_EXISTED_CQL_TABLE))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withColumn(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput("TextQuoted"),
                  DataTypes.TEXT) // doubleQuoted column
              .withColumn("\"setColumn\"", DataTypes.setOf(DataTypes.TEXT, false)) // set column
              .withColumn(
                  "\"mapColumn\"",
                  DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT, false)) // map column
              .withColumn("\"listColumn\"", DataTypes.listOf(DataTypes.TEXT, false)); // list column
      assertThat(executeCqlStatement(createTable.build())).isTrue();

      // Create an index on the doubleQuoted column "TextQuoted"
      String createTextIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS \"idx_textQuoted\" ON \"%s\".\"%s\" (\"TextQuoted\") USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createTextIndexCql))).isTrue();

      // Create an index on the entire set
      String createSetIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_set ON \"%s\".\"%s\" (\"setColumn\") USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createSetIndexCql))).isTrue();

      // Create an index on the entire list
      String createListIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_list ON \"%s\".\"%s\" (\"listColumn\") USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createListIndexCql))).isTrue();

      // Create an index on the keys of the map
      String createMapKeyIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_keys ON \"%s\".\"%s\" (KEYS(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createMapKeyIndexCql))).isTrue();

      // Create an index on the values of the map
      String createMapValueIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_values ON \"%s\".\"%s\" (VALUES(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createMapValueIndexCql))).isTrue();

      // Create an index on the entries of the map
      String createMapEntryIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_entries ON \"%s\".\"%s\" (ENTRIES(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, PRE_EXISTED_CQL_TABLE);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createMapEntryIndexCql))).isTrue();
    }

    @Test
    @Order(2)
    public void listIndexesWithDefinition() {
      // TODO, currently ApiIndexType treats index on map/set/list as unsupported, so the index on
      // these columns have UNKNOWN column in the definition
      var expected_idx_set =
              """
                {
                    "name": "idx_set",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_set ON \\"%s\\".\\"%s\\" (values(\\"setColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, PRE_EXISTED_CQL_TABLE);
      var expected_idx_map_values =
              """
                {
                    "name": "idx_map_values",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_values ON \\"%s\\".\\"%s\\" (values(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, PRE_EXISTED_CQL_TABLE);
      var expected_idx_map_keys =
              """
                {
                    "name": "idx_map_keys",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_keys ON \\"%s\\".\\"%s\\" (keys(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, PRE_EXISTED_CQL_TABLE);
      var expected_idx_map_entries =
              """
                {
                    "name": "idx_map_entries",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_entries ON \\"%s\\".\\"%s\\" (entries(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, PRE_EXISTED_CQL_TABLE);
      var expected_idx_list =
              """
                {
                    "name": "idx_list",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_list ON \\"%s\\".\\"%s\\" (values(\\"listColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, PRE_EXISTED_CQL_TABLE);
      var expected_idx_quoted =
          """
                 {
                     "name": "idx_textQuoted",
                     "definition": {
                         "column": "TextQuoted",
                         "options": {
                         }
                     },
                     "indexType": "regular"
                 }
                 """;
      assertTableCommand(keyspaceName, PRE_EXISTED_CQL_TABLE)
          .templated()
          .listIndexes(true)
          .wasSuccessful()
          .body("status.indexes", hasSize(6))
          .body(
              "status.indexes",
              containsInAnyOrder( // Validate that the indexes are in any order
                  jsonEquals(expected_idx_set),
                  jsonEquals(expected_idx_map_keys),
                  jsonEquals(expected_idx_map_values),
                  jsonEquals(expected_idx_map_entries),
                  jsonEquals(expected_idx_list),
                  jsonEquals(expected_idx_quoted)));
    }
  }
}
