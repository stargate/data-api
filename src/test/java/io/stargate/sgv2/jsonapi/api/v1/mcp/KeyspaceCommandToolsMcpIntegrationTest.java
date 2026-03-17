package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

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

  private static final String TYPE_NAME = "new_type";

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
          assertStatusOnlyOk());
    }

    @Test
    @Order(2)
    void testFindCollectionsToolCallAfterCreateCollection() {
      callToolAndAssert(
          CommandName.Names.FIND_COLLECTIONS,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray collections = status.getJsonArray("collections");
                assertNotNull(collections, "Collections array should not be null");
                assertTrue(
                    collections.contains(COLLECTION_NAME),
                    "New created Collection should be in the list");
              }));
    }

    @Test
    @Order(3)
    void testDeleteCollectionToolCall() {
      callToolAndAssert(
          CommandName.Names.DELETE_COLLECTION,
          Map.of("keyspace", keyspaceName, "collection", COLLECTION_NAME),
          assertStatusOnlyOk());
    }

    @Test
    @Order(4)
    void testFindCollectionToolCallAfterDeleteCollection() {
      callToolAndAssert(
          CommandName.Names.FIND_COLLECTIONS,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray collections = status.getJsonArray("collections");
                assertNotNull(collections, "Collections array should not be null");
                assertFalse(
                    collections.contains(COLLECTION_NAME),
                    "Collection should be dropped and not in the list");
              }));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateFindAndDropTableToolCall {
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
          assertStatusOnlyOk());
    }

    @Test
    @Order(2)
    void testFindTablesToolCallAfterCreateTable() {
      callToolAndAssert(
          CommandName.Names.LIST_TABLES,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray tables = status.getJsonArray("tables");
                assertNotNull(tables, "Table array should not be null");
                assertTrue(tables.contains(TABLE_NAME), "New created Table should be in the list");
              }));
    }

    @Test
    @Order(3)
    void testDropTableToolCall() {
      callToolAndAssert(
          CommandName.Names.DROP_TABLE,
          Map.of("keyspace", keyspaceName, "table", TABLE_NAME),
          assertStatusOnlyOk());
    }

    @Test
    @Order(4)
    void testFindTablesToolCallAfterDropTable() {
      callToolAndAssert(
          CommandName.Names.LIST_TABLES,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray tables = status.getJsonArray("tables");
                assertNotNull(tables, "Tables array should not be null");
                assertFalse(
                    tables.contains(TABLE_NAME), "Table should be dropped and not in the list");
              }));
    }
  }

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class AlterCreateFindAndDropTypeToolCall {
    @Test
    @Order(1)
    void testCreateTypeToolCall() {
      callToolAndAssert(
          CommandName.Names.CREATE_TYPE,
          Map.of(
              "keyspace",
              keyspaceName,
              "name",
              TYPE_NAME,
              "definition",
              Map.of("fields", Map.of("city", "text", "postcode", "int"))),
          assertStatusOnlyOk());
    }

    @Test
    @Order(2)
    void testListTypeToolCallAfterCreateType() {
      callToolAndAssert(
          CommandName.Names.LIST_TYPES,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray types = status.getJsonArray("types");
                assertNotNull(types, "Types array should not be null");
                assertTrue(types.contains(TYPE_NAME), "New created Type should be in the list");
              }));
    }

    @Test
    @Order(3)
    void testAlterTypeToolCall() {
      callToolAndAssert(
          CommandName.Names.ALTER_TYPE,
          Map.of(
              "keyspace",
              keyspaceName,
              "name",
              TYPE_NAME,
              "rename",
              Map.of("fields", Map.of("postcode", "zipcode")),
              "add",
              Map.of("fields", Map.of("is_active", "boolean"))),
          assertStatusOnlyOk());
    }

    @Test
    @Order(4)
    void testListTypeToolCallAfterAlterType() {
      callToolAndAssert(
          CommandName.Names.LIST_TYPES,
          Map.of("keyspace", keyspaceName, "options", Map.of("explain", true)),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray types = status.getJsonArray("types");
                assertNotNull(types, "Types array should not be null");
                assertFalse(types.isEmpty(), "Types array should not be empty");

                JsonObject fields =
                    types.getJsonObject(0).getJsonObject("definition").getJsonObject("fields");

                // check the field has been renamed ("postcode" -> "zipcode")
                assertNull(
                    fields.getJsonObject("postcode"), "Old field 'postcode' should be renamed");
                assertNotNull(
                    fields.getJsonObject("zipcode"), "Renamed field 'zipcode' should exist");

                // check the new field was added
                assertNotNull(
                    fields.getJsonObject("is_active"), "Added field 'is_active' should exist");
              }));
    }

    @Test
    @Order(5)
    void testDropTypeToolCall() {
      callToolAndAssert(
          CommandName.Names.DROP_TYPE,
          Map.of("keyspace", keyspaceName, "name", TYPE_NAME),
          assertStatusOnlyOk());
    }

    @Test
    @Order(6)
    void testListTypeToolCallAfterDropType() {
      callToolAndAssert(
          CommandName.Names.LIST_TYPES,
          Map.of("keyspace", keyspaceName),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray types = status.getJsonArray("types");
                assertNotNull(types, "Types array should not be null");
                assertFalse(
                    types.contains(TYPE_NAME), "New Type should be dropped and not in the list");
              }));
    }
  }
}
