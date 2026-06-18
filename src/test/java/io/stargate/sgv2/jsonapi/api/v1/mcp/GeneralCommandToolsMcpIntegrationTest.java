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
          CommandName.Names.CREATE_KEYSPACE, Map.of("name", EXTRA_KEYSPACE), assertStatusOnlyOk());
    }

    @Test
    @Order(2)
    void testFindKeyspaceToolCallAfterCreateKeyspace() {
      callToolAndAssert(
          CommandName.Names.FIND_KEYSPACES,
          Map.of(),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray keyspaces = status.getJsonArray("keyspaces");
                assertNotNull(keyspaces, "Keyspaces array should not be null");
                assertTrue(
                    keyspaces.contains(EXTRA_KEYSPACE),
                    "New created Keyspace should be in the list");
              }));
    }

    @Test
    @Order(3)
    void testDropKeyspaceToolCall() {
      callToolAndAssert(
          CommandName.Names.DROP_KEYSPACE, Map.of("name", EXTRA_KEYSPACE), assertStatusOnlyOk());
    }

    @Test
    @Order(4)
    void testFindKeyspaceToolCallAfterDropKeyspace() {
      callToolAndAssert(
          CommandName.Names.FIND_KEYSPACES,
          Map.of(),
          assertStatusOnlyWithJson(
              status -> {
                JsonArray keyspaces = status.getJsonArray("keyspaces");
                assertNotNull(keyspaces, "Keyspaces array should not be null");
                assertFalse(
                    keyspaces.contains(EXTRA_KEYSPACE),
                    "Keyspace should be dropped and not in the list");
              }));
    }
  }

  @Test
  void findEmbeddingProvidersToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND_EMBEDDING_PROVIDERS,
        Map.of(),
        assertStatusOnlyWithJson(
            status -> {
              JsonObject providers = status.getJsonObject("embeddingProviders");
              assertNotNull(providers, "embeddingProviders should not be null");
              assertNotNull(providers.getJsonObject("nvidia"), "nvidia provider should be present");
            }));
  }

  @Test
  void findRerankingProvidersToolCall() {
    callToolAndAssert(
        CommandName.Names.FIND_RERANKING_PROVIDERS,
        Map.of(),
        assertStatusOnlyWithJson(
            status -> {
              JsonObject providers = status.getJsonObject("rerankingProviders");
              assertNotNull(providers, "rerankingProviders should not be null");
              assertNotNull(providers.getJsonObject("nvidia"), "nvidia provider should be present");
            }));
  }
}
