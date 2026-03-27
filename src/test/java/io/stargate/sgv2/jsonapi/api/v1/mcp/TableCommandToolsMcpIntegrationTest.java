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

  @Test
  @Order(6)
  void testInsertOneToolCall() {
    callToolAndAssert(
        CommandName.Names.INSERT_ONE,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            tableName,
            "document",
            Map.of(
                "id", "1",
                "category", "A",
                "name", "Alice",
                "age", 25,
                "city", "NYC",
                "active", true,
                "score", 9.5,
                "description", "A senior engineer",
                "embedding", List.of(0.1, 0.2, 0.3, 0.4, 0.5))),
        assertStatusOnlyWithJson(
            status -> {
              // Table insertOne returns composite primary key as a nested array
              JsonArray insertedIds = status.getJsonArray("insertedIds");
              assertNotNull(insertedIds, "insertedIds should not be null");
              assertEquals(1, insertedIds.size());
              // Each entry is an array of PK values: ["1", "A"]
              JsonArray pk = insertedIds.getJsonArray(0);
              assertNotNull(pk, "Primary key array should not be null");
              assertTrue(pk.contains("1"), "Partition key 'id=1' should be present");
              assertTrue(pk.contains("A"), "Clustering key 'category=A' should be present");
              // Verify primaryKeySchema reflects the table's PK columns and types
              JsonObject primaryKeySchema = status.getJsonObject("primaryKeySchema");
              assertNotNull(primaryKeySchema, "primaryKeySchema should not be null");
              assertNotNull(primaryKeySchema.getJsonObject("id"), "id column schema should exist");
              assertNotNull(
                  primaryKeySchema.getJsonObject("category"),
                  "category column schema should exist");
            }));
  }

  @Test
  @Order(7)
  void testInsertManyToolCall() {
    callToolAndAssert(
        CommandName.Names.INSERT_MANY,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            tableName,
            "documents",
            List.of(
                Map.of(
                    "id", "2",
                    "category", "A",
                    "name", "Bob",
                    "age", 30,
                    "city", "LA",
                    "active", true,
                    "score", 7.0,
                    "description", "A junior developer",
                    "embedding", List.of(0.2, 0.3, 0.4, 0.5, 0.6)),
                Map.of(
                    "id", "3",
                    "category", "B",
                    "name", "Charlie",
                    "age", 35,
                    "city", "NYC",
                    "active", false,
                    "score", 5.5,
                    "description", "A senior manager",
                    "embedding", List.of(0.3, 0.4, 0.5, 0.6, 0.7)),
                Map.of(
                    "id", "4",
                    "category", "B",
                    "name", "Diana",
                    "age", 28,
                    "city", "Chicago",
                    "active", true,
                    "score", 8.0,
                    "description", "A product designer",
                    "embedding", List.of(0.4, 0.5, 0.6, 0.7, 0.8)))),
        assertStatusOnlyWithJson(
            status -> {
              // Table insertMany also returns insertedIds as array of PK arrays, plus
              // primaryKeySchema
              JsonArray insertedIds = status.getJsonArray("insertedIds");
              assertNotNull(insertedIds, "insertedIds should not be null");
              assertEquals(3, insertedIds.size(), "All 3 rows should be inserted");
              // Verify primaryKeySchema is present and well-formed
              JsonObject primaryKeySchema = status.getJsonObject("primaryKeySchema");
              assertNotNull(primaryKeySchema, "primaryKeySchema should not be null");
              assertNotNull(primaryKeySchema.getJsonObject("id"), "id column schema should exist");
              assertNotNull(
                  primaryKeySchema.getJsonObject("category"),
                  "category column schema should exist");
            }));
  }

  @Test
  @Order(8)
  void testFindOneToolCall() {
    // Filter by primary key (id + category)
    callToolAndAssert(
        CommandName.Names.FIND_ONE,
        Map.of(
            "keyspace", keyspaceName,
            "collection", tableName,
            "filter", Map.of("id", "1", "category", "A")),
        assertDataAndStatus(
            data -> {
              JsonObject doc = data.getJsonObject("document");
              assertNotNull(doc, "document should not be null");
              assertEquals("1", doc.getString("id"));
              assertEquals("A", doc.getString("category"));
              assertEquals("Alice", doc.getString("name"));
              assertEquals("NYC", doc.getString("city"));
              assertEquals(25, doc.getInteger("age"));
              assertTrue(doc.getBoolean("active"));
              // Verify vector field is returned as an array
              JsonArray embedding = doc.getJsonArray("embedding");
              assertNotNull(embedding, "embedding should not be null");
              assertEquals(5, embedding.size());
            },
            status -> {
              // Table findOne returns projectionSchema describing all returned columns
              JsonObject projectionSchema = status.getJsonObject("projectionSchema");
              assertNotNull(projectionSchema, "projectionSchema should not be null");
              assertNotNull(projectionSchema.getJsonObject("id"), "id schema should exist");
              assertNotNull(
                  projectionSchema.getJsonObject("category"), "category schema should exist");
              // Verify vector column schema includes dimension and apiSupport
              JsonObject embeddingSchema = projectionSchema.getJsonObject("embedding");
              assertNotNull(embeddingSchema, "embedding schema should exist");
              assertEquals("vector", embeddingSchema.getString("type"));
              assertEquals(5, embeddingSchema.getInteger("dimension"));
            }));
  }

  @Test
  @Order(9)
  void testFindToolCall() {
    // Use the SAI index on "city" to filter rows where city=NYC
    callToolAndAssert(
        CommandName.Names.FIND,
        Map.of(
            "keyspace", keyspaceName,
            "collection", tableName,
            "filter", Map.of("city", "NYC")),
        assertDataAndStatus(
            data -> {
              JsonArray docs = data.getJsonArray("documents");
              assertNotNull(docs, "documents should not be null");
              // Alice (id=1, cat=A) and Charlie (id=3, cat=B) are both in NYC
              assertEquals(2, docs.size(), "Should find exactly 2 documents in NYC");
              // Verify returned doc fields are well-formed
              JsonObject firstDoc = docs.getJsonObject(0);
              assertNotNull(firstDoc.getString("id"), "id should not be null");
              assertNotNull(firstDoc.getString("city"), "city should not be null");
              assertEquals("NYC", firstDoc.getString("city"));
            },
            status -> {
              // Table find also returns projectionSchema
              JsonObject projectionSchema = status.getJsonObject("projectionSchema");
              assertNotNull(projectionSchema, "projectionSchema should not be null");
              assertNotNull(projectionSchema.getJsonObject("id"), "id schema should exist");
              assertNotNull(projectionSchema.getJsonObject("city"), "city schema should exist");
              // Verify vector column schema
              JsonObject embeddingSchema = projectionSchema.getJsonObject("embedding");
              assertNotNull(embeddingSchema, "embedding schema should exist");
              assertEquals("vector", embeddingSchema.getString("type"));
              assertEquals(5, embeddingSchema.getInteger("dimension"));
            }));
  }

  @Test
  @Order(10)
  void testFindWithVectorSortToolCall() {
    // Use vector sort via the vector index on "embedding"
    callToolAndAssert(
        CommandName.Names.FIND,
        Map.of(
            "keyspace", keyspaceName,
            "collection", tableName,
            "sort", Map.of("embedding", List.of(0.1, 0.2, 0.3, 0.4, 0.5))),
        assertDataAndStatus(
            data -> {
              JsonArray docs = data.getJsonArray("documents");
              assertNotNull(docs, "documents should not be null");
              // All 4 rows should be returned, sorted by vector similarity
              assertEquals(4, docs.size(), "All 4 documents should be returned via vector sort");
              // Alice (id=1) has embedding [0.1,0.2,0.3,0.4,0.5], identical to the query vector,
              // so she should rank first
              JsonObject firstDoc = docs.getJsonObject(0);
              assertEquals("1", firstDoc.getString("id"), "Alice should be the closest match");
              assertEquals("A", firstDoc.getString("category"));
              // Verify the embedding field is returned and has correct dimension
              JsonArray embedding = firstDoc.getJsonArray("embedding");
              assertNotNull(embedding, "embedding should not be null");
              assertEquals(5, embedding.size());
            },
            status -> {
              // projectionSchema is returned for table find operations
              JsonObject projectionSchema = status.getJsonObject("projectionSchema");
              assertNotNull(projectionSchema, "projectionSchema should not be null");
              JsonObject embeddingSchema = projectionSchema.getJsonObject("embedding");
              assertNotNull(embeddingSchema, "embedding schema should exist");
              assertEquals("vector", embeddingSchema.getString("type"));
              assertEquals(5, embeddingSchema.getInteger("dimension"));
            }));
  }

  @Test
  @Order(11)
  void testUpdateOneToolCall() {
    // Update score of Bob (id=2, category=A)
    callToolAndAssert(
        CommandName.Names.UPDATE_ONE,
        Map.of(
            "keyspace",
            keyspaceName,
            "collection",
            tableName,
            "filter",
            Map.of("id", "2", "category", "A"),
            "update",
            Map.of("$set", Map.of("score", 7.5, "active", false))),
        assertStatusOnlyWithJson(
            status -> {
              assertEquals(1, status.getInteger("matchedCount"));
              assertEquals(1, status.getInteger("modifiedCount"));
            }));
  }

  @Test
  @Order(12)
  void testDeleteOneToolCall() {
    // Delete Bob (id=2, category=A) by primary key filter
    callToolAndAssert(
        CommandName.Names.DELETE_ONE,
        Map.of(
            "keyspace", keyspaceName,
            "collection", tableName,
            "filter", Map.of("id", "2", "category", "A")),
        assertStatusOnlyWithJson(status -> assertEquals(-1, status.getInteger("deletedCount"))));
  }

  @Test
  @Order(13)
  void testDeleteManyToolCall() {
    // Delete all rows in category=B (Charlie + Diana)
    callToolAndAssert(
        CommandName.Names.DELETE_MANY,
        Map.of(
            "keyspace", keyspaceName,
            "collection", tableName),
        assertStatusOnlyWithJson(status -> assertEquals(-1, status.getInteger("deletedCount"))));
  }

  @Test
  @Order(17)
  void testDropIndexToolCall() {
    // Drop the regular index created earlier
    // this one is under KeyspaceCommand
    callToolAndAssert(
        CommandName.Names.DROP_INDEX,
        Map.of("keyspace", keyspaceName, "name", REGULAR_INDEX_NAME),
        assertStatusOnlyOk());
  }

  @Test
  @Order(18)
  void testListIndexesAfterDropIndexToolCall() {
    // Verify the dropped index is no longer listed
    callToolAndAssert(
        CommandName.Names.LIST_INDEXES,
        Map.of("keyspace", keyspaceName, "table", tableName),
        assertStatusOnlyWithJson(
            status -> {
              JsonArray indexes = status.getJsonArray("indexes");
              assertNotNull(indexes, "indexes array should not be null");
              assertFalse(
                  indexes.contains(REGULAR_INDEX_NAME), "Dropped index should not be in the list");
            }));
  }
}
