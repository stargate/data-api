package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
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

  @BeforeAll
  public void createTable() {
    createKeyspace(keyspaceName);
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
    callToolAndAssert(
        CommandName.Names.CREATE_INDEX,
        Map.of(
            "keyspace",
            keyspaceName,
            "table",
            tableName,
            "indexName",
            "regular_index",
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
            "text_index",
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
            "vector_index",
            "definition",
            Map.of("column", "embedding")),
        assertStatusOnlyOk());
  }
}
