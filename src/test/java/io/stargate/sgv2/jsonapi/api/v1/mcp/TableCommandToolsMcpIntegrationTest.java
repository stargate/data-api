package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * MCP integration tests for table-related commands in {@link CollectionCommandTools}. Uses the
 * Streamable HTTP transport via McpAssured to test collection-level MCP tools end-to-end.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TableCommandToolsMcpIntegrationTest extends McpIntegrationTestBase {

  // Index names used across tests
  private static final String REGULAR_INDEX_NAME = "idx_city";
  private static final String TEXT_INDEX_NAME = "idx_text_name";
  private static final String VECTOR_INDEX_NAME = "idx_vector_embedding";

  @BeforeAll
  public void createTable() {
    createKeyspace(keyspaceName);
    // Create a table with composite primary key and multiple typed columns
    createTable(
        keyspaceName,
        tableName,
        Map.of(
            "columns",
            Map.of(
                "id", "text",
                "category", "text",
                "name", "text",
                "age", "int",
                "city", "text",
                "active", "boolean",
                "score", "float"),
            "primaryKey",
            Map.of("partitionBy", List.of("id"), "partitionSort", Map.of("category", 1))));
  }

  @AfterAll
  public void deleteTable() {
    dropTable(keyspaceName, tableName);
    dropKeyspace(keyspaceName);
  }

  @Test
  @Order(1)
  void testAlterTableAddColumnsToolCall() {
    // Add a text column "description" and a vector column "embedding" (dim=5)
    callToolAndAssert(
        CommandName.Names.ALTER_TABLE,
        Map.of(
            "keyspace",
            keyspaceName,
            "table",
            tableName,
            "operation",
            Map.of(
                "add",
                Map.of(
                    "columns",
                    Map.of(
                        "description",
                        Map.of("type", "text"),
                        "embedding",
                        Map.of("type", "vector", "dimension", 5))))),
        assertStatusOnlyOk());
  }

  @Test
  @Order(2)
  void testCreateIndexToolCall() {
    // Create a regular SAI index on the "city" column
    callToolAndAssert(
        CommandName.Names.CREATE_INDEX,
        Map.of(
            "keyspace",
            keyspaceName,
            "table",
            tableName,
            "indexName",
            REGULAR_INDEX_NAME,
            "definition",
            Map.of("column", "city")),
        assertStatusOnlyOk());
  }

  @Test
  @Order(3)
  void testCreateTextIndexToolCall() {
    // Create a text (lexical) index on the "name" column
    callToolAndAssert(
        CommandName.Names.CREATE_TEXT_INDEX,
        Map.of(
            "keyspace",
            keyspaceName,
            "table",
            tableName,
            "indexName",
            TEXT_INDEX_NAME,
            "definition",
            Map.of("column", "name")),
        assertStatusOnlyOk());
  }

  @Test
  @Order(4)
  void testCreateVectorIndexToolCall() {
    // Create a vector index on the "embedding" column (added via alterTable in Order 1)
    callToolAndAssert(
        CommandName.Names.CREATE_VECTOR_INDEX,
        Map.of(
            "keyspace",
            keyspaceName,
            "table",
            tableName,
            "indexName",
            VECTOR_INDEX_NAME,
            "definition",
            Map.of("column", "embedding")),
        assertStatusOnlyOk());
  }

  @Test
  @Order(5)
  void testListIndexesToolCall() {
    // All three indexes should be present
    callToolAndAssert(
        CommandName.Names.LIST_INDEXES,
        Map.of("keyspace", keyspaceName, "table", tableName),
        assertStatusOnlyWithJson(
            status -> {
              JsonArray indexes = status.getJsonArray("indexes");
              assertNotNull(indexes, "indexes array should not be null");
              assertTrue(indexes.contains(REGULAR_INDEX_NAME), "Regular index should exist");
              assertTrue(indexes.contains(TEXT_INDEX_NAME), "Text index should exist");
              assertTrue(indexes.contains(VECTOR_INDEX_NAME), "Vector index should exist");
            }));
  }
}
