package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkiverse.mcp.server.MetaKey;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * MCP integration tests for {@link KeyspaceCommandTools}. Uses the Streamable HTTP transport via
 * McpAssured to test keyspace-level MCP tools end-to-end.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
public class KeyspaceCommandToolsMcpIntegrationTest extends McpIntegrationTestBase {

  private static final String COLLECTION_NAME = "new_col";

  private static final String TABLE_NAME = "new_table";

  @BeforeAll
  public void createKeyspace() {
    createKeyspace(keyspaceName);
  }

  @AfterAll
  public void dropKeyspace() {
    dropKeyspace(keyspaceName);
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CollectionRelatedToolCall {
    @Test
    @Order(1)
    void testCreateCollectionToolCall() {
      callToolAndAssert(
          CommandName.Names.CREATE_COLLECTION,
          Map.of("keyspace", keyspaceName, "collection", COLLECTION_NAME),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
            assertThat(response.content()).isEmpty();
          });
    }

    @Test
    @Order(2)
    void testFindCollectionsToolCallAfterCreateCollection() {
      callToolAndAssert(
          CommandName.Names.FIND_COLLECTIONS,
          Map.of("keyspace", keyspaceName),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());

            // status data is in _meta
            var status = (JsonObject) response._meta().get(MetaKey.of("status"));
            assertNotNull(status, "Status should not be null");
            JsonArray collections = status.getJsonArray("collections");
            assertNotNull(collections, "Collections array should not be null");
            assertTrue(
                collections.contains(COLLECTION_NAME),
                "New created Collection should be in the list");
          });
    }

    @Test
    @Order(3)
    void testDeleteCollectionToolCall() {
      callToolAndAssert(
          CommandName.Names.DELETE_COLLECTION,
          Map.of("keyspace", keyspaceName, "collection", COLLECTION_NAME),
          response -> {
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
          });
    }

    @Test
    @Order(4)
    void testFindCollectionToolCallAfterDeleteCollection() {
      callToolAndAssert(
          CommandName.Names.FIND_COLLECTIONS,
          Map.of("keyspace", keyspaceName),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());

            // check the new collection is dropped
            var status = (JsonObject) response._meta().get(MetaKey.of("status"));
            assertNotNull(status, "Status should not be null");
            JsonArray collections = status.getJsonArray("collections");
            assertNotNull(collections, "Collections array should not be null");
            assertFalse(
                collections.contains(COLLECTION_NAME),
                "Collection should be dropped and not in the list");
          });
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class TableRelatedToolCall {
    @Test
    @Order(1)
    void testCreateTableToolCall() {
      callToolAndAssert(
          CommandName.Names.CREATE_TABLE,
          Map.of(
              "keyspace",
              keyspaceName,
              "table",
              TABLE_NAME,
              "definition",
              Map.of("columns", Map.of("id", "text"), "primaryKey", "id")),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
            assertThat(response.content()).isEmpty();
          });
    }

    @Test
    @Order(2)
    void testFindTablesToolCallAfterCreateTable() {
      callToolAndAssert(
          CommandName.Names.LIST_TABLES,
          Map.of("keyspace", keyspaceName),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());

            // status data is in _meta
            var status = (JsonObject) response._meta().get(MetaKey.of("status"));
            assertNotNull(status, "Status should not be null");
            JsonArray collections = status.getJsonArray("tables");
            assertNotNull(collections, "Table array should not be null");
            assertTrue(collections.contains(TABLE_NAME), "New created Table should be in the list");
          });
    }

    @Test
    @Order(3)
    void testDropTableToolCall() {
      callToolAndAssert(
          CommandName.Names.DROP_TABLE,
          Map.of("keyspace", keyspaceName, "table", TABLE_NAME),
          response -> {
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
          });
    }

    @Test
    @Order(4)
    void testFindTablesToolCallAfterDropTable() {
      callToolAndAssert(
          CommandName.Names.LIST_TABLES,
          Map.of("keyspace", keyspaceName),
          response -> {
            // status only response, no error, no structureContent, no content
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());

            // check the new collection is dropped
            var status = (JsonObject) response._meta().get(MetaKey.of("status"));
            assertNotNull(status, "Status should not be null");
            JsonArray collections = status.getJsonArray("tables");
            assertNotNull(collections, "Tables array should not be null");
            assertFalse(
                collections.contains(TABLE_NAME), "Table should be dropped and not in the list");
          });
    }
  }
}
