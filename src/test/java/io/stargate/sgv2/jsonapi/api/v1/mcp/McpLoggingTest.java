package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(McpLoggingTest.LoggingProfile.class)
class McpLoggingTest {
  static final Path LOG_FILE = Path.of("target/mcp-logging-test.log");
  @InjectMock CommandProcessor commandProcessor;

  @Test
  void normalLogsExcludePayloadsAndCredentials() throws IOException {
    long start = Files.size(LOG_FILE);
    given()
        .contentType(ContentType.JSON)
        .accept("application/json, text/event-stream")
        .header("Token", "synthetic-header-secret")
        .header("Authorization", "Bearer synthetic-access-secret")
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "tools/list")
        .body(
            Map.of(
                "jsonrpc", "2.0",
                "id", "synthetic-rpc-id-secret",
                "method", "tools/list",
                "params",
                    Map.of(
                        "_meta",
                        Map.of(
                            "io.modelcontextprotocol/protocolVersion", "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities", Map.of(),
                            "document", "synthetic-document-secret",
                            "query", "synthetic-query-secret",
                            "refresh_token", "synthetic-refresh-secret",
                            "code_verifier", "synthetic-pkce-secret",
                            "client_secret", "synthetic-registration-secret"))))
        .post(mcpPath() + "?code=synthetic-code-secret")
        .then()
        .statusCode(200);

    assertThat(logsSince(start))
        .contains("MCP request completed", "method=tools/list", "status=200")
        .doesNotContain("synthetic-");
  }

  @Test
  void errorLogsExcludeMalformedHeadersAndBodies() throws IOException {
    long start = Files.size(LOG_FILE);
    given()
        .contentType(ContentType.JSON)
        .accept("synthetic-invalid-accept-secret")
        .header("Token", "synthetic-header-secret")
        .body("{}")
        .post(mcpPath())
        .then()
        .statusCode(400);
    given()
        .contentType(ContentType.JSON)
        .accept("application/json, text/event-stream")
        .header("Token", "synthetic-header-secret")
        .body("{\"access_token\":synthetic-malformed-body-secret}")
        .post(mcpPath());

    assertThat(logsSince(start))
        .contains("MCP event details redacted", "status=400")
        .doesNotContain("synthetic-");
  }

  @Test
  void returnedDocumentsStayOutOfLogsButCommandCategoriesRemain() throws IOException {
    var result =
        CommandResult.singleDocumentBuilder(RequestTracing.NO_OP)
            .addDocument(
                JsonNodeFactory.instance.objectNode().put("document", "synthetic-response-secret"))
            .build();
    when(commandProcessor.processCommand(any(), any())).thenReturn(Uni.createFrom().item(result));
    long start = Files.size(LOG_FILE);

    callProviderTool();

    assertThat(logsSince(start))
        .contains(
            "MCP command completed", "command=findEmbeddingProviders", "isError=false", "requestId=")
        .doesNotContain("synthetic-");
  }

  @Test
  void asynchronousCommandErrorsStayOutOfLogs() throws IOException {
    when(commandProcessor.processCommand(any(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(1)
                .onItem()
                .delayIt()
                .by(Duration.ofMillis(1))
                .map(
                    ignored ->
                        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                            .addThrowable(
                                new IllegalArgumentException("synthetic-command-error-secret"))
                            .build()));
    long start = Files.size(LOG_FILE);

    callProviderTool();

    assertThat(logsSince(start))
        .contains("MCP command completed", "command=findEmbeddingProviders", "isError=true")
        .doesNotContain("synthetic-");
  }

  @Test
  void asynchronousCommandFailuresStayOutOfLogs() throws IOException {
    when(commandProcessor.processCommand(any(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(1)
                .onItem()
                .delayIt()
                .by(Duration.ofMillis(1))
                .map(
                    ignored -> {
                      throw new IllegalStateException("synthetic-command-failure-secret");
                    }));
    long start = Files.size(LOG_FILE);

    callProviderTool();

    assertThat(logsSince(start))
        .contains("MCP command failed", "command=findEmbeddingProviders")
        .doesNotContain("synthetic-");
  }

  private void callProviderTool() {
    McpHttpTestSupport.request("tools/call")
        .header("Token", "synthetic-header-secret")
        .header("Mcp-Name", "findEmbeddingProviders")
        .body(McpHttpTestSupport.toolPayload(1, "findEmbeddingProviders", Map.of()))
        .post(mcpPath())
        .then()
        .statusCode(200);
  }

  static String logsSince(long start) throws IOException {
    byte[] bytes = Files.readAllBytes(LOG_FILE);
    return new String(
        Arrays.copyOfRange(bytes, Math.toIntExact(start), bytes.length), StandardCharsets.UTF_8);
  }

  @Test
  void subsequentHttpRequestsKeepTheirOrdinaryLogs() throws IOException {
    normalLogsExcludePayloadsAndCredentials();
    long start = Files.size(LOG_FILE);
    assertThat(given().get("/v1/logging-isolation-check").statusCode()).isIn(401, 404);
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertThat(logsSince(start)).contains("/v1/logging-isolation-check HTTP/1.1"));
  }

  private String mcpPath() {
    // RestAssured's base path already includes quarkus.http.root-path.
    return ConfigProvider.getConfig().getValue("quarkus.mcp.server.http.root-path", String.class);
  }

  public static class LoggingProfile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.log.file.enabled", "true",
          "quarkus.log.file.path", LOG_FILE.toString(),
          "quarkus.log.file.format", "%p %c %m%e%n",
          "stargate.feature.flags.mcp", "true",
          "quarkus.http.access-log.enabled", "true",
          "quarkus.log.category.\"io.quarkus.mcp.server.traffic\".level", "INFO");
    }
  }
}
