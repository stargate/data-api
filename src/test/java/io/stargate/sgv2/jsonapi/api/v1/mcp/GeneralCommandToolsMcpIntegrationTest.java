package io.stargate.sgv2.jsonapi.api.v1.mcp;

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
 * MCP integration tests for {@link GeneralCommandTools}. Uses the Streamable HTTP transport via
 * McpAssured to test all general-level MCP tools end-to-end.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class GeneralCommandToolsMcpIntegrationTest extends McpIntegrationTestBase {

  private static final String EXTRA_KEYSPACE = "new_ks";

  @Nested
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class CreateFindAndDropKeyspaceToolCall {
    @Test
    @Order(1)
    void testCreateKeyspaceToolCall() {
      callToolAndAssert(
          CommandName.Names.CREATE_KEYSPACE,
          Map.of("name", EXTRA_KEYSPACE),
          response -> {
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
          });
    }

    @Test
    @Order(2)
    void testFindKeyspaceToolCall() {
      callToolAndAssert(
          CommandName.Names.FIND_KEYSPACES,
          Map.of(),
          response -> {
            // check mcp response
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());

            // check the new keyspace is there
            var status = (JsonObject) response._meta().get(MetaKey.of("status"));
            assertNotNull(status, "Status should not be null");
            JsonArray keyspaces = status.getJsonArray("keyspaces");
            assertNotNull(keyspaces, "Keyspaces array should not be null");
            assertTrue(
                keyspaces.contains(EXTRA_KEYSPACE), "New created Keyspace should be in the list");
          });
    }

    @Test
    @Order(3)
    void testDropKeyspaceToolCall() {
      callToolAndAssert(
          CommandName.Names.DROP_KEYSPACE,
          Map.of("name", EXTRA_KEYSPACE),
          response -> {
            assertFalse(response.isError());
            assertNotNull(response._meta());
            assertNull(response.structuredContent());
          });
    }
  }

  @Test
  void findEmbeddingProvidersToolCall() {
    callToolAndAssert(
        "findEmbeddingProviders",
        Map.of(),
        response -> {
          assertFalse(response.isError());
          assertNotNull(response._meta());
          assertNull(response.structuredContent());
        });
  }

  @Test
  void findRerankingProvidersToolCall() {
    callToolAndAssert(
        "findRerankingProviders",
        Map.of(),
        response -> {
          assertFalse(response.isError());
          assertNotNull(response._meta());
          assertNull(response.structuredContent());
        });
  }
}
