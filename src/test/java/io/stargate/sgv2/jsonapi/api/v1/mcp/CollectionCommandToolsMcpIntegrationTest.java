package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

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
                "city",
                "NYC",
                "active",
                true,
                "$vector",
                List.of(0.25, 0.25, 0.25, 0.25, 0.25))),
        assertStatusOnlyWithJson(
            status -> {
              JsonArray insertedIds = status.getJsonArray("insertedIds");
              assertNotNull(insertedIds, "insertedIds should not be null");
              assertTrue(insertedIds.contains("1"));
            }));
  }

  @Test
  @Order(2)
  void testInsertManyToolCall() {
    callToolAndAssert(
        CommandName.Names.INSERT_MANY,
        Map.of(
            "keyspace", keyspaceName,
            "collection", collectionName,
            "documents",
                List.of(
                    Map.of(
                        "_id",
                        "2",
                        "name",
                        "Bob",
                        "age",
                        30,
                        "city",
                        "LA",
                        "active",
                        true,
                        "$vector",
                        List.of(0.10, 0.10, 0.10, 0.10, 0.10)),
                    Map.of(
                        "_id",
                        "3",
                        "name",
                        "Charlie",
                        "age",
                        35,
                        "city",
                        "NYC",
                        "active",
                        false,
                        "$vector",
                        List.of(0.50, 0.50, 0.50, 0.50, 0.50)),
                    Map.of(
                        "_id",
                        "4",
                        "name",
                        "Diana",
                        "age",
                        28,
                        "city",
                        "Chicago",
                        "active",
                        true,
                        "$vector",
                        List.of(0.75, 0.75, 0.75, 0.75, 0.75)))),
        assertStatusOnlyWithJson(
            status -> {
              JsonArray insertedIds = status.getJsonArray("insertedIds");
              assertNotNull(insertedIds, "insertedIds should not be null");
              assertEquals(3, insertedIds.size());
              assertTrue(insertedIds.contains("2"));
              assertTrue(insertedIds.contains("3"));
              assertTrue(insertedIds.contains("4"));
            }));
  }

  @Test
  @Order(3)
  void testCountDocumentsToolCall() {
    callToolAndAssert(
        CommandName.Names.COUNT_DOCUMENTS,
        Map.of("keyspace", keyspaceName, "collection", collectionName),
        assertStatusOnlyWithJson(status -> assertEquals(4, status.getInteger("count"))));
  }

  @Test
  @Order(3)
  void testEstimatedDocumentCountToolCall() {
    callToolAndAssert(
        CommandName.Names.ESTIMATED_DOCUMENT_COUNT,
        Map.of("keyspace", keyspaceName, "collection", collectionName),
        assertStatusOnlyWithJson(status -> assertTrue(status.getInteger("count") >= 0)));
  }

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
