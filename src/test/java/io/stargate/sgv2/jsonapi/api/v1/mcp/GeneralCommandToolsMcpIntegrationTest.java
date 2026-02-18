package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraPassword;
import static io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils.getCassandraUsername;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.test.McpAssured;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.MultiMap;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.*;

/**
 * MCP integration tests for {@link GeneralCommandTools}. Uses McpAssured/McpSseTestClient to
 * exercise MCP tool calls via SSE transport, with the same auth mechanism as the REST API tests.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GeneralCommandToolsMcpIntegrationTest {

  private static final String TEST_KEYSPACE = "mcp_test_ks";

  private McpAssured.McpSseTestClient mcpClient;

  @BeforeAll
  public void setUp() {
    // Build MCP SSE client with auth headers matching the REST API test auth
    mcpClient =
        McpAssured.newSseClient()
            .setAdditionalHeaders(
                initMessage -> {
                  MultiMap headers = MultiMap.caseInsensitiveMultiMap();
                  headers.add(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, buildAuthToken());
                  headers.add(
                      HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
                      CustomITEmbeddingProvider.TEST_API_KEY);
                  return headers;
                })
            .build()
            .connect();
  }

  @AfterAll
  public void tearDown() {
    // Drop the test keyspace (best-effort cleanup)
    if (mcpClient != null && mcpClient.isConnected()) {
      try {
        mcpClient
            .when()
            .toolsCall("dropKeyspace")
            .withArguments(Map.of("name", TEST_KEYSPACE))
            .withAssert(response -> {})
            .send();
      } catch (Exception e) {
        // Ignore cleanup failures
      }
      mcpClient.disconnect();
    }
  }

  /**
   * Build the auth token string in the same format used by REST API integration tests: {@code
   * Cassandra:base64(username):base64(password)}
   */
  private static String buildAuthToken() {
    return "Cassandra:"
        + Base64.getEncoder().encodeToString(getCassandraUsername().getBytes())
        + ":"
        + Base64.getEncoder().encodeToString(getCassandraPassword().getBytes());
  }

  @Nested
  @Order(1)
  class ToolsDiscovery {

    @Test
    void shouldListAllGeneralTools() {
      mcpClient
          .when()
          .toolsList(
              page -> {
                assertThat(page.size()).isGreaterThanOrEqualTo(5);
                assertThat(page.findByName("createKeyspace")).isNotNull();
                assertThat(page.findByName("dropKeyspace")).isNotNull();
                assertThat(page.findByName("findKeyspaces")).isNotNull();
                assertThat(page.findByName("findEmbeddingProviders")).isNotNull();
                assertThat(page.findByName("findRerankingProviders")).isNotNull();
              });
    }
  }

  @Nested
  @Order(2)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  class KeyspaceOperations {

    @Test
    @Order(1)
    void shouldCreateKeyspace() {
      mcpClient
          .when()
          .toolsCall(
              "createKeyspace",
              Map.of("name", TEST_KEYSPACE),
              response -> {
                assertThat(response.isError()).isFalse();
                assertThat(response.content()).isNotEmpty();
                String text = ((TextContent) response.firstContent()).text();
                assertThat(text).contains("\"ok\"");
              });
    }

    @Test
    @Order(2)
    void shouldFindKeyspaces() {
      mcpClient
          .when()
          .toolsCall(
              "findKeyspaces",
              response -> {
                assertThat(response.isError()).isFalse();
                assertThat(response.content()).isNotEmpty();
                String text = ((TextContent) response.firstContent()).text();
                // The result should contain keyspace names
                assertThat(text).contains("keyspaces");
              });
    }

    @Test
    @Order(3)
    void shouldCreateKeyspaceWithOptions() {
      String ksWithOptions = TEST_KEYSPACE + "_opts";
      mcpClient
          .when()
          .toolsCall("createKeyspace")
          .withArguments(
              Map.of(
                  "name",
                  ksWithOptions,
                  "options",
                  Map.of(
                      "replication", Map.of("class", "SimpleStrategy", "replication_factor", 1))))
          .withAssert(
              response -> {
                assertThat(response.isError()).isFalse();
                String text = ((TextContent) response.firstContent()).text();
                assertThat(text).contains("\"ok\"");
              })
          .send();

      // Cleanup: drop the keyspace we just created
      mcpClient
          .when()
          .toolsCall("dropKeyspace")
          .withArguments(Map.of("name", ksWithOptions))
          .withAssert(response -> assertThat(response.isError()).isFalse())
          .send();
    }

    @Test
    @Order(4)
    void shouldDropKeyspace() {
      // Create a temporary keyspace to drop
      String tempKs = TEST_KEYSPACE + "_drop";
      mcpClient
          .when()
          .toolsCall(
              "createKeyspace",
              Map.of("name", tempKs),
              response -> assertThat(response.isError()).isFalse());

      mcpClient
          .when()
          .toolsCall(
              "dropKeyspace",
              Map.of("name", tempKs),
              response -> {
                assertThat(response.isError()).isFalse();
                String text = ((TextContent) response.firstContent()).text();
                assertThat(text).contains("\"ok\"");
              });
    }
  }

  @Nested
  @Order(3)
  class ProviderOperations {

    @Test
    void shouldFindEmbeddingProviders() {
      mcpClient
          .when()
          .toolsCall(
              "findEmbeddingProviders",
              response -> {
                assertThat(response.isError()).isFalse();
                assertThat(response.content()).isNotEmpty();
                String text = ((TextContent) response.firstContent()).text();
                assertThat(text).contains("embeddingProviders");
              });
    }

    @Test
    void shouldFindRerankingProviders() {
      mcpClient
          .when()
          .toolsCall(
              "findRerankingProviders",
              response -> {
                assertThat(response.isError()).isFalse();
                assertThat(response.content()).isNotEmpty();
                String text = ((TextContent) response.firstContent()).text();
                assertThat(text).contains("rerankingProviders");
              });
    }
  }
}
