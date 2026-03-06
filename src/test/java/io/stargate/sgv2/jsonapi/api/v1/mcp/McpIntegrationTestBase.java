package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkiverse.mcp.server.test.McpAssured;
import io.quarkiverse.mcp.server.test.McpAssured.McpStreamableTestClient;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.core.MultiMap;
import java.net.URI;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Abstract base class for MCP integration tests. Provides a shared MCP client instance,
 * authentication, and utility methods for invoking MCP tools via the Streamable HTTP transport.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class McpIntegrationTestBase {

  private static final String MCP_PATH = "/v1/mcp";

  /** MCP Assured cannot automatically resolve the URI (like Rest Assured) */
  private static final String MCP_HOSTNAME = "http://localhost:";

  /** Test keyspace name, with a random suffix for isolation. */
  protected final String keyspaceName =
      "mcp_ks_" + RandomStringUtils.insecure().nextAlphanumeric(8).toLowerCase();

  /** Test collection name, with a random suffix for isolation. */
  protected final String collectionName =
      "mcp_col_" + RandomStringUtils.insecure().nextAlphanumeric(8).toLowerCase();

  /** Shared MCP client instance, connected once per test class. */
  protected McpStreamableTestClient mcpClient;

  /**
   * Initializes the shared MCP client before all tests in the class. Connects to the local test
   * server using Streamable HTTP transport with authentication headers.
   */
  @BeforeAll
  void setUpMcpClient() {
    mcpClient =
        McpAssured.newStreamableClient()
            .setBaseUri(URI.create(MCP_HOSTNAME + getTestPort()))
            .setMcpPath(MCP_PATH)
            .setAdditionalHeaders(msg -> authHeaders())
            .build()
            .connect();
  }

  /** Disconnects and releases the shared MCP client after all tests in the class have run. */
  @AfterAll
  void tearDownMcpClient() {
    if (mcpClient != null) {
      mcpClient.disconnect();
      mcpClient = null;
    }
  }

  protected int getTestPort() {
    try {
      return ConfigProvider.getConfig().getValue("quarkus.http.test-port", Integer.class);
    } catch (Exception e) {
      return Integer.parseInt(System.getProperty("quarkus.http.test-port"));
    }
  }

  /** Build authentication headers matching the Token header format used by the REST API. */
  protected MultiMap authHeaders() {
    String credential =
        "Cassandra:"
            + Base64.getEncoder().encodeToString(getCassandraUsername().getBytes())
            + ":"
            + Base64.getEncoder().encodeToString(getCassandraPassword().getBytes());
    return MultiMap.caseInsensitiveMultiMap()
        .add(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, credential);
  }

  /** Create a keyspace via the MCP createKeyspace tool. */
  protected void createKeyspace(String keyspace) {
    callToolAndAssert(
        "createKeyspace", Map.of("name", keyspace), response -> assertFalse(response.isError()));
  }

  /** Drop a keyspace via the MCP dropKeyspace tool. */
  protected void dropKeyspace(String keyspace) {
    callToolAndAssert(
        "dropKeyspace", Map.of("name", keyspace), response -> assertFalse(response.isError()));
  }

  /** Create a collection via the MCP createCollection tool. */
  protected void createCollection(String keyspace, String collection) {
    callToolAndAssert(
        "createCollection",
        Map.of("keyspace", keyspace, "collection", collection),
        response -> assertFalse(response.isError()));
  }

  /** Delete a collection via the MCP deleteCollection tool */
  protected void deleteCollection(String keyspace, String collection) {
    callToolAndAssert(
        "deleteCollection",
        Map.of("keyspace", keyspace, "collection", collection),
        response -> assertFalse(response.isError()));
  }

  /**
   * Execute an MCP tool call and assert the response using the shared client.
   *
   * @param toolName the MCP tool name to invoke
   * @param args the tool arguments
   * @param assertFn assertion function for the ToolResponse
   */
  protected void callToolAndAssert(
      String toolName, Map<String, Object> args, Consumer<ToolResponse> assertFn) {
    mcpClient.when().toolsCall(toolName, args, assertFn).thenAssertResults();
  }
}
