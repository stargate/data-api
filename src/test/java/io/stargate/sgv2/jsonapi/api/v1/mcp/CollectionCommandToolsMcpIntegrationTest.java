package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
  @Order(4)
  void testEstimatedDocumentCountToolCall() {
    callToolAndAssert(
        CommandName.Names.ESTIMATED_DOCUMENT_COUNT,
        Map.of("keyspace", keyspaceName, "collection", collectionName),
        assertStatusOnlyWithJson(status -> assertTrue(status.getInteger("count") >= 0)));
  }

  @Test
  @Order(5)
  void testFindOneToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND_ONE,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            collectionName,
            "filter",
            Map.of("active", true, "name", "Alice"),
            "sort",
            Map.of("$vector", List.of(0.25, 0.25, 0.25, 0.25, 0.25))),
        assertDataOnly(
            data -> {
              JsonObject doc = data.getJsonObject("document");
              assertNotNull(doc, "document should not be null");
              assertEquals("1", doc.getString("_id"));
              assertEquals("Alice", doc.getString("name"));
            }));
  }

  @Test
  @Order(6)
  void testFindToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            collectionName,
            "filter",
            Map.of("city", "NYC"),
            "sort",
            Map.of("$vector", List.of(0.25, 0.25, 0.25, 0.25, 0.25))),
        assertDataOnly(
            data -> {
              JsonArray docs = data.getJsonArray("documents");
              assertNotNull(docs);
              assertEquals(2, docs.size());
            }));
  }

  @Test
  @Order(7)
  void testFindOneAndUpdateToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND_ONE_AND_UPDATE,
        Map.of(
            "keyspace", keyspaceName,
            "collection", collectionName,
            "filter", Map.of("_id", "2"),
            "update", Map.of("$set", Map.of("age", 31)),
            "options", Map.of("returnDocument", "after")),
        assertDataAndStatus(
            data -> {
              JsonObject doc = data.getJsonObject("document");
              assertNotNull(doc);
              assertEquals("2", doc.getString("_id"));
              assertEquals(31, doc.getInteger("age"));
            },
            status -> {
              assertEquals(1, status.getInteger("matchedCount"));
              assertEquals(1, status.getInteger("modifiedCount"));
            }));
  }

  @Test
  @Order(8)
  void testFindOneAndReplaceToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND_ONE_AND_REPLACE,
        Map.of(
            "keyspace", keyspaceName,
            "collection", collectionName,
            "filter", Map.of("_id", "3"),
            "replacement",
                Map.of("_id", "3", "name", "Charlie_Replaced", "age", 36, "city", "Boston"),
            "options", Map.of("returnDocument", "after")),
        assertDataAndStatus(
            data -> {
              JsonObject doc = data.getJsonObject("document");
              assertNotNull(doc);
              assertEquals("Charlie_Replaced", doc.getString("name"));
              assertEquals("Boston", doc.getString("city"));
            },
            status -> {
              assertEquals(1, status.getInteger("matchedCount"));
              assertEquals(1, status.getInteger("modifiedCount"));
            }));
  }

  @Test
  @Order(9)
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
