package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * MCP integration tests for collection-related command in {@link CollectionCommandTools}. Uses the
 * Streamable HTTP transport via McpAssured to test collection-level MCP tools end-to-end.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CollectionCommandToolsMcpIntegrationTest extends McpIntegrationTestBase {

  @BeforeAll
  public void createCollection() {
    createKeyspace(keyspaceName);
    createCollection(keyspaceName, collectionName);
  }

  @AfterAll
  public void deleteCollection() {
    deleteCollection(keyspaceName, collectionName);
    dropKeyspace(keyspaceName);
  }

  @Test
  @Order(1)
  void testInsertOneToolCall() {
    callToolAndAssert(
        CommandName.Names.INSERT_ONE,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            collectionName,
            "document",
            Map.of(
                "_id",
                "1",
                "name",
                "Alice",
                "age",
                25,
                "$vector",
                List.of(0.25, 0.25, 0.25, 0.25, 0.25))),
        assertStatusOnlyWithJson(
            status -> {
              JsonArray insertedIds = status.getJsonArray("insertedIds");
              assertNotNull(insertedIds, "insertedIds should not be null");
              assertEquals("1", insertedIds.getValue(0), "insertedIds should contain 1");
            }));
  }

  @Test
  @Order(2)
  void testInsertManyToolCall() {}

  @Test
  @Order(3)
  void testCountDocumentsToolCall() {}

  @Test
  @Order(3)
  void testEstimatedDocumentCountToolCall() {}

  @Test
  @Order(3)
  void testFindOneToolCall() {}

  @Test
  @Order(4)
  void testFindToolCall() {}

  @Test
  @Order(4)
  void testFindOneAndUpdateToolCall() {}

  @Test
  @Order(4)
  void testFindOneAndReplaceToolCall() {}

  @Test
  @Order(4)
  void testFindOneAndDeleteToolCall() {}

  // ????????
  @Test
  @Order(4)
  void testFindAndRerankToolCall() {}

  @Test
  @Order(4)
  void testUpdateOneToolCall() {}

  @Test
  @Order(4)
  void testUpdateManyToolCall() {}

  @Test
  @Order(4)
  void testDeleteOneToolCall() {}

  @Test
  @Order(4)
  void testDeleteManyToolCall() {}
}
