package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.completeResult;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.readableResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
            "MCP command completed",
            "command=findEmbeddingProviders",
            "isError=false",
            "requestId=")
        .doesNotContain("synthetic-");
  }

  @Test
  void diagnosticTraceAndCompletionLogsShareAGeneratedRequestId() throws IOException {
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              return Uni.createFrom()
                  .item(CommandResult.statusOnlyBuilder(context.requestTracing()).build());
            });
    var clientRequestId = "11111111-1111-4111-8111-111111111111";
    var payload =
        new JsonObject(McpHttpTestSupport.toolPayload(1, "findEmbeddingProviders", Map.of()))
            .copy();
    payload
        .getJsonObject("params")
        .getJsonObject("_meta")
        .put("requestId", clientRequestId)
        .put("mcpRequestId", clientRequestId);
    long start = Files.size(LOG_FILE);

    var response =
        McpHttpTestSupport.request("tools/call")
            .header("Token", "synthetic-header-secret")
            .header("Mcp-Name", "findEmbeddingProviders")
            .header(ApiFeature.REQUEST_TRACING.httpHeaderName(), "true")
            .header("X-Request-ID", clientRequestId)
            .header("mcpRequestId", clientRequestId)
            .body(payload.encode())
            .post(mcpPath());

    var requestId =
        readableResult(completeResult(response, 1))
            .getJsonObject("status")
            .getJsonObject("trace")
            .getJsonObject("TraceSession")
            .getString("requestId");
    assertThat(requestId).matches("[a-f0-9-]{36}").isNotEqualTo(clientRequestId);
    assertThat(logsSince(start))
        .contains(
            "MCP request completed requestId=" + requestId,
            "MCP command completed requestId="
                + requestId
                + " command=findEmbeddingProviders isError=false")
        .doesNotContain(clientRequestId);
  }

  @Test
  void asynchronousCommandErrorsStayOutOfLogs() throws IOException {
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation ->
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
        .thenAnswer(
            invocation ->
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

  @Test
  void interleavedHttpCallsKeepTheirRequestTracingAndLogContext() throws Exception {
    var firstCompletion = new CompletableFuture<Integer>();
    var secondCompletion = new CompletableFuture<Integer>();
    var completions =
        Map.of("synthetic-token-1", firstCompletion, "synthetic-token-2", secondCompletion);
    var started = new LinkedBlockingQueue<PendingCall>();
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              var callbackMdc = new CompletableFuture<Map<String, String>>();
              var call = new PendingCall(context, MDC.getCopyOfContextMap(), callbackMdc);
              return Uni.createFrom()
                  .<Integer>emitter(
                      emitter -> {
                        completions
                            .get(context.requestContext().authToken())
                            .thenAccept(emitter::complete);
                        // Both requests remain open until the test completes their futures.
                        started.add(call);
                      })
                  .map(
                      value -> {
                        callbackMdc.complete(MDC.getCopyOfContextMap());
                        context.requestTracing().maybeTrace("completed-call-" + value);
                        LoggerFactory.getLogger(McpLoggingTest.class)
                            .info("synthetic-driver-payload-secret");
                        var result =
                            CommandResult.statusOnlyBuilder(context.requestTracing())
                                .addStatus(CommandStatus.OK, value);
                        if (value == 2) {
                          result.addThrowable(
                              new IllegalArgumentException("synthetic-call-2-error"));
                        }
                        return result.build();
                      });
            });

    try (var clients = Executors.newFixedThreadPool(2);
        var worker = Executors.newSingleThreadExecutor()) {
      // Start the worker before either request can contribute inherited context.
      worker.submit(() -> MDC.put(McpRequestLogging.REQUEST_ID, "worker-context")).get();
      long start = Files.size(LOG_FILE);
      try {
        var firstResponse =
            clients.submit(
                () ->
                    tracedProviderRequest(1).header("Token", "synthetic-token-1").post(mcpPath()));
        var first = awaitStarted(started, firstResponse);
        var secondResponse =
            clients.submit(
                () ->
                    tracedProviderRequest(2).header("Token", "synthetic-token-2").post(mcpPath()));
        var second = awaitStarted(started, secondResponse);

        assertThat(first.context().requestContext()).isNotSameAs(second.context().requestContext());
        assertThat(first.context().requestContext().requestId())
            .isNotEqualTo(second.context().requestContext().requestId());
        assertThat(first.context().requestTracing().enabled()).isTrue();
        assertThat(second.context().requestTracing().enabled()).isTrue();
        assertThat(first.context().requestTracing().getSession().orElseThrow())
            .isNotSameAs(second.context().requestTracing().getSession().orElseThrow());
        assertThat(first.logRequestId())
            .matches("[a-f0-9-]{36}")
            .isNotEqualTo(second.logRequestId());
        assertThat(second.logRequestId()).matches("[a-f0-9-]{36}");

        assertThat(
                worker
                    .submit(
                        () -> {
                          secondCompletion.complete(2);
                          return MDC.get(McpRequestLogging.REQUEST_ID);
                        })
                    .get(10, TimeUnit.SECONDS))
            .isEqualTo("worker-context");
        var secondResult = completeResult(secondResponse.get(10, TimeUnit.SECONDS), 2);
        assertThat(firstResponse.isDone()).isFalse();
        assertThat(firstCompletion.isDone()).isFalse();
        assertThat(
                worker
                    .submit(
                        () -> {
                          firstCompletion.complete(1);
                          return MDC.get(McpRequestLogging.REQUEST_ID);
                        })
                    .get(10, TimeUnit.SECONDS))
            .isEqualTo("worker-context");
        var firstResult = completeResult(firstResponse.get(10, TimeUnit.SECONDS), 1);

        assertIsolatedResult(first, firstResult, 1);
        assertIsolatedResult(second, secondResult, 2);
        var logs = logsSince(start);
        for (var call : List.of(first, second)) {
          assertThat(logs).contains("MCP request completed requestId=" + call.logRequestId());
          traceMetadata(call == first ? 1 : 2)
              .values()
              .forEach(value -> assertThat(logs).doesNotContain(value));
        }
        assertThat(logs)
            .contains(
                "MCP command completed requestId="
                    + first.logRequestId()
                    + " command=findEmbeddingProviders isError=false",
                "MCP command completed requestId="
                    + second.logRequestId()
                    + " command=findEmbeddingProviders isError=true",
                "MCP event details redacted")
            .doesNotContain("synthetic-", "requestId=null");
      } finally {
        firstCompletion.complete(1);
        secondCompletion.complete(2);
      }
    }
  }

  @Test
  void traceMetadataCannotAuthenticateAToolCall() throws IOException {
    long start = Files.size(LOG_FILE);
    var response = tracedProviderRequest(1).post(mcpPath());

    assertThat(response.statusCode()).isEqualTo(401);
    verifyNoInteractions(commandProcessor);
    assertThat(response.asString()).doesNotContain("synthetic-");
    var logs = logsSince(start);
    assertThat(logs).contains("status=401").doesNotContain("synthetic-");
    traceMetadata(1)
        .values()
        .forEach(
            value -> {
              assertThat(response.asString()).doesNotContain(value);
              assertThat(logs).doesNotContain(value);
            });
  }

  private static PendingCall awaitStarted(
      LinkedBlockingQueue<PendingCall> started, Future<Response> response) throws Exception {
    var call = started.poll(10, TimeUnit.SECONDS);
    if (call == null && response.isDone()) {
      throw new AssertionError(
          "The request ended before reaching the command processor: " + response.get().asString());
    }
    assertThat(call).as("The command processor received the request").isNotNull();
    return call;
  }

  private void assertIsolatedResult(PendingCall call, JsonObject result, int id) throws Exception {
    assertThat(result.getBoolean("isError")).isEqualTo(id == 2);
    var status = readableResult(result).getJsonObject("status");
    assertThat(status.getInteger("ok")).isEqualTo(id);
    var trace = status.getJsonObject("trace").getJsonObject("TraceSession");
    assertThat(trace.getString("requestId"))
        .isEqualTo(call.context().requestContext().requestId())
        .isEqualTo(call.logRequestId());
    assertThat(trace.getJsonArray("events")).hasSize(1);
    assertThat(
            trace
                .getJsonArray("events")
                .getJsonObject(0)
                .getJsonObject("TraceEvent")
                .getString("message"))
        .isEqualTo("completed-call-" + id);
    var callbackMdc = call.callbackMdc().get(10, TimeUnit.SECONDS);
    assertThat(callbackMdc).containsEntry(McpRequestLogging.REQUEST_ID, call.logRequestId());
    for (var mdc : List.of(call.startMdc(), callbackMdc)) {
      assertThat(mdc)
          .doesNotContainKeys("traceId", "spanId", "traceparent", "tracestate", "baggage");
    }
    for (int otherId : List.of(1, 2)) {
      for (String value : traceMetadata(otherId).values()) {
        assertThat(result.encode()).doesNotContain(value);
        assertThat(call.startMdc()).doesNotContainValue(value);
        assertThat(callbackMdc).doesNotContainValue(value);
      }
    }
  }

  private RequestSpecification tracedProviderRequest(int id) {
    var payload =
        new JsonObject(McpHttpTestSupport.toolPayload(id, "findEmbeddingProviders", Map.of()))
            .copy();
    var meta = payload.getJsonObject("params").getJsonObject("_meta");
    traceMetadata(id).forEach(meta::put);
    return McpHttpTestSupport.request("tools/call")
        .header("Mcp-Name", "findEmbeddingProviders")
        .header(ApiFeature.REQUEST_TRACING.httpHeaderName(), "true")
        .headers(traceMetadata(id))
        .body(payload.encode());
  }

  private static Map<String, String> traceMetadata(int id) {
    return Map.of(
        "traceparent",
            "00-" + String.valueOf(id).repeat(32) + "-" + String.valueOf(id).repeat(16) + "-01",
        "tracestate", "vendor=synthetic-state-" + id,
        "baggage", "token=synthetic-baggage-token-" + id);
  }

  private record PendingCall(
      CommandContext<?> context,
      Map<String, String> startMdc,
      CompletableFuture<Map<String, String>> callbackMdc) {
    String logRequestId() {
      return startMdc.get(McpRequestLogging.REQUEST_ID);
    }
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
