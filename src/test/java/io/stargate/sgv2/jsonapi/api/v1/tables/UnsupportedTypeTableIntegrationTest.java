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
import io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import org.junit.jupiter.api.*;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class, restrictToAnnotatedClass = false)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UnsupportedTypeTableIntegrationTest extends AbstractTableIntegrationTestBase {

  /**
   * Data API support for frozen set/map/list column: CreateTable(false), Insert(true), Read(true),
   * Filter(false)
   */
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class FrozenMapSetList {
    private static final String TABLE_FROZEN_MAP_SET_LIST = "table_frozen_map_set_list";
    private static final String ONLY_ONE_ID = "id1";
    private static final String INSERT_DOC =
            """
              {
                "id": "%s",
                "frozen_set": ["1", "2", "1"],
                "frozen_map": {"1":1, "2":2},
                "frozen_list": ["1", "2", "1"]
              }
              """
            .formatted(ONLY_ONE_ID);

    // Notice, different from INSERT_DOC because of the uniqueness of Set values.
    private static final String RETURN_DOC =
            """
              {
                "id": "%s",
                "frozen_set": ["1", "2"],
                "frozen_map": { "1":1,"2":2},
                "frozen_list": ["1", "2", "1"]
              }
              """
            .formatted(ONLY_ONE_ID);

    /**
     * Data API support for frozen map/set/list: CreateTable(False), Insert(True), Read(True),
     * Filter(False)
     */
    @Test
    @Order(1)
    public final void createDefaultTablesAndIndexes() {
      // Build the CREATE TABLE statement
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_FROZEN_MAP_SET_LIST))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withColumn("text", DataTypes.TEXT) // Regular column
              .withColumn("frozen_set", DataTypes.setOf(DataTypes.TEXT, true)) // Frozen set column
              .withColumn(
                  "frozen_map",
                  DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT, true)) // Frozen map column
              .withColumn(
                  "frozen_list", DataTypes.listOf(DataTypes.TEXT, true)); // Frozen list column

      assertThat(executeCqlStatement(createTable.build())).isTrue();
    }

    /*
    Although countCommand currently does not support table, the command still should fail gracefully as 200,
    when it is executed against a table that has unsupported DataType
     */
    @Test
    @Order(2)
    public void countDocumentsAgainstTable() {
      DataApiCommandSenders.assertTableCommand(keyspaceName, TABLE_FROZEN_MAP_SET_LIST)
          .postCount()
          .hasSingleApiError(
              RequestException.Code.UNSUPPORTED_TABLE_COMMAND, RequestException.class);
    }

    @Test
    @Order(2)
    public void listTables() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
          .body("status.tables", notNullValue())
          .body("status.tables", hasSize(greaterThan(0)));
    }

    @Test
    @Order(3)
    public void insertOnFrozen() {
      assertTableCommand(keyspaceName, TABLE_FROZEN_MAP_SET_LIST)
          .templated()
          .insertOne(INSERT_DOC)
          .wasSuccessful();
    }

    @Test
    @Order(4)
    public void readOnFrozen() {
      assertTableCommand(keyspaceName, TABLE_FROZEN_MAP_SET_LIST)
          .templated()
          .findOne(ImmutableMap.of("id", ONLY_ONE_ID), null)
          .wasSuccessful()
          .hasJSONField("data.document", RETURN_DOC);
    }

    @Test
    @Order(4)
    public void canNotFilterOnFrozen() {
      assertTableCommand(keyspaceName, TABLE_FROZEN_MAP_SET_LIST)
          .templated()
          .findOne(ImmutableMap.of("frozen_set", "123"), null)
          .hasSingleApiError(
              FilterException.Code.UNSUPPORTED_FILTERING_FOR_COLUMN_TYPES,
              FilterException.class,
              "Filtering is only supported on primitive data types such as `text` not on container types such as `list`, `set`, `map`, or `vector`");
    }
  }

  /**
   * Data API support for counter column: CreateTable(False), Insert(false), Read(True),
   * Filter(True)
   */
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class Counter {
    private static final String TABLE_COUNTER = "tableCounter";

    @Test
    @Order(1)
    public final void createDefaultTablesAndIndexes() {
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_COUNTER))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withColumn("counter", DataTypes.COUNTER); // Counter Column

      assertThat(executeCqlStatement(createTable.build())).isTrue();
    }

    // In Cassandra, you cannot use an INSERT statement directly for counters.
    // Instead, counter columns require a special kind of update operation.
    // Counters in Cassandra are designed to increment or decrement a value,
    // and these operations are performed using the UPDATE statement, not INSERT.
    @Test
    @Order(2)
    public final void insertCounter() {
      String DOC =
              """
                      {
                        "id": "%s",
                        "counter": "%s"
                      }
                      """
              .formatted("1", 1);
      assertTableCommand(keyspaceName, TABLE_COUNTER)
          .templated()
          .insertOne(DOC)
          .hasSingleApiError(
              DocumentException.Code.UNSUPPORTED_COLUMN_TYPES,
              DocumentException.class,
              "Only supported column types can be included when inserting a document into a table");
    }

    // In Cassandra, Cannot set the value of counter column counter_value (counters can only be
    // incremented/decremented, not set)
    // Data API tables do not support incremented/decremented currently
    @Test
    @Order(3)
    public final void updateCounter() {
      ImmutableMap<String, Object> filterOnRow = ImmutableMap.of("id", "1");

      ImmutableMap<String, Object> updateSet =
          ImmutableMap.of("$set", ImmutableMap.of("counter", 1));

      assertTableCommand(keyspaceName, TABLE_COUNTER)
          .templated()
          .updateOne(filterOnRow, updateSet)
          .hasSingleApiError(
              DatabaseException.Code.INVALID_DATABASE_QUERY,
              DatabaseException.class,
              "Cannot set the value of counter column counter");
    }

    // TODO filter on counter, INVALID_FILTER_COLUMN_VALUES
    @Test
    @Order(4)
    public final void filterCounter() {
      assertTableCommand(keyspaceName, TABLE_COUNTER)
          .templated()
          .findOne(ImmutableMap.of("counter", 1), null)
          .hasSingleApiError(
              FilterException.Code.INVALID_FILTER_COLUMN_VALUES,
              FilterException.class,
              "Only values that are supported by the column data type can be included");
    }
  }

  /**
   * Data API support for TIMEUUID column: CreateTable(False), Insert(True), Read(True),
   * Filter(True)
   */
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class TimeUuid {
    private static final String TABLE_TIMEUUID = "table_time_uuid";
    private static final String ONLY_ONE_ID = "id1";
    private static final String ONLY_ONE_TIMEUUID = "f8442890-b74d-11ef-b843-c639d7f47ce1";
    private static final String DOC =
            """
              {
                "id": "%s",
                "created_at": "%s"
              }
              """
            .formatted(ONLY_ONE_ID, ONLY_ONE_TIMEUUID);

    @Test
    @Order(1)
    public final void createDefaultTablesAndIndexes() {
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_TIMEUUID))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withColumn("created_at", DataTypes.TIMEUUID); // TIMEUUID Column

      assertThat(executeCqlStatement(createTable.build())).isTrue();
    }

    @Test
    @Order(2)
    public void listTables() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
          .body("status.tables", notNullValue())
          .body("status.tables", hasSize(greaterThan(0)));
    }

    @Test
    @Order(2)
    public final void insertTimeUuid() {
      assertTableCommand(keyspaceName, TABLE_TIMEUUID).templated().insertOne(DOC).wasSuccessful();
    }

    @Test
    @Order(3)
    public final void ReadTimeUuid() {
      assertTableCommand(keyspaceName, TABLE_TIMEUUID)
          .templated()
          .findOne(ImmutableMap.of("id", ONLY_ONE_ID), null)
          .wasSuccessful()
          .hasJSONField("data.document", DOC);
    }

    @Test
    @Order(4)
    public final void FilterTimeUuid() {
      assertTableCommand(keyspaceName, TABLE_TIMEUUID)
          .templated()
          .findOne(ImmutableMap.of("created_at", ONLY_ONE_TIMEUUID), null)
          .wasSuccessful()
          .hasJSONField("data.document", DOC);
    }
  }

  /**
   * Data API support for static column: CreateTable(False), Insert(True), Read(True), Filter(True)
   *
   * <p>note, a static column is a special type of column that has a single value shared by all rows
   * within the same partition. Static columns are only useful (and thus allowed) if the table has
   * at least one clustering column
   *
   * <p>CREATE TABLE keyspace_name.table_static ( id text, static_int int STATIC, static_text text
   * STATIC, PRIMARY KEY (id) );
   */
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class StaticColumn {
    private static final String TABLE_STATIC = "table_static";
    private static final String ONLY_ONE_ID = "id1";
    private static final String ONLY_ONE_TEXT = "text1";
    private static final int ONLY_STATIC_INT = 1;
    private static final String ONLY_STATIC_TEXT = "static_text_1";
    private static final String DOC =
            """
              {
                "id": "%s",
                "name": "%s",
                "static_int": %s,
                "static_text": "%s"
              }
              """
            .formatted(ONLY_ONE_ID, ONLY_ONE_TEXT, ONLY_STATIC_INT, ONLY_STATIC_TEXT);

    @Test
    @Order(1)
    public final void createDefaultTablesAndIndexes() {
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_STATIC))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withClusteringColumn("name", DataTypes.TEXT)
              .withStaticColumn("static_int", DataTypes.INT) // static int column
              .withStaticColumn("static_text", DataTypes.TEXT); // static text column;

      assertThat(executeCqlStatement(createTable.build())).isTrue();
    }

    @Test
    @Order(2)
    public void listTables() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
          .body("status.tables", notNullValue())
          .body("status.tables", hasSize(greaterThan(0)));
    }

    @Test
    @Order(2)
    public final void insertStaticColumn() {
      assertTableCommand(keyspaceName, TABLE_STATIC).templated().insertOne(DOC).wasSuccessful();
    }

    @Test
    @Order(3)
    public final void ReadStatic() {
      assertTableCommand(keyspaceName, TABLE_STATIC)
          .templated()
          .findOne(ImmutableMap.of("id", ONLY_ONE_ID), null)
          .wasSuccessful()
          .hasJSONField("data.document", DOC);
    }

    @Test
    @Order(4)
    public final void FilterStatic() {
      assertTableCommand(keyspaceName, TABLE_STATIC)
          .templated()
          .findOne(ImmutableMap.of("static_int", ONLY_STATIC_INT), null)
          .wasSuccessful()
          .hasJSONField("data.document", DOC);

      assertTableCommand(keyspaceName, TABLE_STATIC)
          .templated()
          .findOne(ImmutableMap.of("static_text", ONLY_STATIC_TEXT), null)
          .wasSuccessful()
          .hasJSONField("data.document", DOC);
    }
  }

  /**
   * Data API does NOT support: <br>
   * 1. create custom index on table (not SAI, legacy secondary index) <br>
   * 2. create value, key, entry indexes on map/set/list <br>
   *
   * <p>But any above unsupported index on a pre-existed table should not break Data API command
   *
   * <p>CREATE TABLE table_unsupported_index ( id TEXT PRIMARY KEY, "TextQuoted" TEXT, "setColumn"
   * SET<TEXT>, "mapColumn" MAP<TEXT, INT>, "listColumn" LIST<TEXT> );
   */
  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  public class UnsupportedIndexOnPreExistedTable {
    private static final String TABLE_WITH_UNSUPPORTED_INDEX = "table_unsupported_index";
    private static final String ONLY_ONE_ID = "id1";
    private static final String ONLY_ONE_TEXT = "text1";
    private static final int ONLY_ONE_INT = 1;
    private static final String INSERT_DOC =
            """
              {
                "id": "%s",
                "TextQuoted": "%s",
                "IntQuoted": %s,
                "setColumn": ["1", "2", "1"],
                "mapColumn": {"1":1, "2":2},
                "listColumn": ["1", "2", "1"]
              }
              """
            .formatted(ONLY_ONE_ID, ONLY_ONE_TEXT, ONLY_ONE_INT);

    // Notice, different from INSERT_DOC because of the uniqueness of Set values.
    private static final String RETURN_DOC =
            """
             {
                "id": "%s",
                "TextQuoted": "%s",
                "IntQuoted": %s,
                "setColumn": ["1", "2"],
                "mapColumn": { "1":1,"2":2},
                "listColumn": ["1", "2", "1"]
              }
             """
            .formatted(ONLY_ONE_ID, ONLY_ONE_TEXT, ONLY_ONE_INT);

    @Test
    @Order(1)
    public final void createPerExistedCqlTable() {
      // Build the CREATE TABLE statement
      CreateTable createTable =
          SchemaBuilder.createTable(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspaceName),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_WITH_UNSUPPORTED_INDEX))
              .withPartitionKey("id", DataTypes.TEXT) // Primary key
              .withColumn(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput("TextQuoted"),
                  DataTypes.TEXT) // doubleQuoted text column
              .withColumn(
                  CqlIdentifierUtil.cqlIdentifierFromUserInput("IntQuoted"),
                  DataTypes.INT) // doubleQuoted int column
              .withColumn("\"setColumn\"", DataTypes.setOf(DataTypes.TEXT, false)) // set column
              .withColumn(
                  "\"mapColumn\"",
                  DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT, false)) // map column
              .withColumn("\"listColumn\"", DataTypes.listOf(DataTypes.TEXT, false)); // list column
      assertThat(executeCqlStatement(createTable.build())).isTrue();

      // Create a Data API supported SAI index on the doubleQuoted column "TextQuoted"
      String createTextIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS \"idx_textQuoted\" ON \"%s\".\"%s\" (\"TextQuoted\") USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createTextIndexCql))).isTrue();

      // Create Legacy Secondary Index on the doubleQuoted column "IntQuoted"
      String createIntIndexCql =
          String.format(
              "CREATE INDEX IF NOT EXISTS \"idx_intQuoted\" ON \"%s\".\"%s\" (\"IntQuoted\")",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createIntIndexCql))).isTrue();

      // Create an index on the entire set
      String createSetIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_set ON \"%s\".\"%s\" (\"setColumn\") USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createSetIndexCql))).isTrue();

      // Create an index on the entire list
      String createListIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_list ON \"%s\".\"%s\" (\"listColumn\") USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createListIndexCql))).isTrue();

      // Create an index on the keys of the map
      String createMapKeyIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_keys ON \"%s\".\"%s\" (KEYS(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createMapKeyIndexCql))).isTrue();

      // Create an index on the values of the map
      String createMapValueIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_values ON \"%s\".\"%s\" (VALUES(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      assertThat(executeCqlStatement(SimpleStatement.newInstance(createMapValueIndexCql))).isTrue();

      // Create an index on the entries of the map
      String createMapEntryIndexCql =
          String.format(
              "CREATE CUSTOM INDEX IF NOT EXISTS idx_map_entries ON \"%s\".\"%s\" (ENTRIES(\"mapColumn\")) USING 'StorageAttachedIndex'",
              keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
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
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_set ON \\"%s\\".%s (values(\\"setColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      var expected_idx_map_values =
              """
                {
                    "name": "idx_map_values",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_values ON \\"%s\\".%s (values(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      var expected_idx_map_keys =
              """
                {
                    "name": "idx_map_keys",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_keys ON \\"%s\\".%s (keys(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      var expected_idx_map_entries =
              """
                {
                    "name": "idx_map_entries",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_map_entries ON \\"%s\\".%s (entries(\\"mapColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      var expected_idx_list =
              """
                {
                    "name": "idx_list",
                    "definition": {
                        "column": "UNKNOWN",
                        "apiSupport": {
                            "createIndex": false,
                            "filter": false,
                            "cqlDefinition": "CREATE CUSTOM INDEX idx_list ON \\"%s\\".%s (values(\\"listColumn\\"))\\nUSING 'StorageAttachedIndex'"
                        }
                    },
                    "indexType": "UNKNOWN"
               }
              """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);
      var expected_idx_quotedText =
              """
                     {
                         "name": "idx_textQuoted",
                         "definition": {
                              "column": "TextQuoted",
                               "options": {}
                           }
                         },
                         "indexType": "regular"
                     }
                     """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);

      var expected_idx_quotedInt =
              """
                 {
                     "name": "idx_intQuoted",
                     "definition": {
                            "column": "UNKNOWN",
                            "apiSupport": {
                                "createIndex": false,
                                "filter": false,
                                "cqlDefinition": "CREATE INDEX \\"idx_intQuoted\\" ON \\"%s\\".%s (\\"IntQuoted\\");"
                            }
                     },
                     "indexType": "UNKNOWN"
                 }
                 """
              .formatted(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX);

      assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX)
          .templated()
          .listIndexes(true)
          .wasSuccessful()
          .body("status.indexes", hasSize(7))
          .body(
              "status.indexes",
              containsInAnyOrder( // Validate that the indexes are in any order
                  jsonEquals(expected_idx_set),
                  jsonEquals(expected_idx_map_keys),
                  jsonEquals(expected_idx_map_values),
                  jsonEquals(expected_idx_map_entries),
                  jsonEquals(expected_idx_list),
                  jsonEquals(expected_idx_quotedText),
                  jsonEquals(expected_idx_quotedInt)));
    }

    @Test
    @Order(2)
    public void listTables() {
      assertNamespaceCommand(keyspaceName)
          .templated()
          .listTables(true)
          .wasSuccessful()
          .body("status.tables", notNullValue())
          .body("status.tables", hasSize(greaterThan(0)));
    }

    @Test
    @Order(3)
    public final void insertTableWithUnsupportedIndex() {
      assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX)
          .templated()
          .insertOne(INSERT_DOC)
          .wasSuccessful();
    }

    @Test
    @Order(4)
    public final void ReadTableWithUnsupportedIndex() {
      assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX)
          .templated()
          .findOne(ImmutableMap.of("id", ONLY_ONE_ID), null)
          .wasSuccessful()
          .hasJSONField("data.document", RETURN_DOC);
    }

    @Test
    @Order(5)
    public final void FilterTableWithUnsupportedIndex() {

      // Note, IntQuoted column has legacy secondary index on it, so when filter on it, we got
      // MISSING_INDEX warning back
      assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX)
          .templated()
          .findOne(ImmutableMap.of("IntQuoted", ONLY_ONE_INT), null)
          .mayHaveSingleWarning(WarningException.Code.MISSING_INDEX)
          .mayFoundSingleDocumentIdByFindOne(null, ONLY_ONE_ID);

      assertTableCommand(keyspaceName, TABLE_WITH_UNSUPPORTED_INDEX)
          .templated()
          .findOne(ImmutableMap.of("mapColumn", "123"), null)
          .hasSingleApiError(
              FilterException.Code.UNSUPPORTED_FILTERING_FOR_COLUMN_TYPES,
              FilterException.class,
              "Filtering is only supported on primitive data types such as `text` not on container types such as `list`, `set`, `map`, or `vector`");
    }
  }
}
