package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.config;
import static io.restassured.RestAssured.given;
import static io.restassured.config.HttpClientConfig.httpClientConfig;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.CURRENT_PROTOCOL_VERSION;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertRpcError;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.AUTH_SERVER;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_ORG;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_SUBJECT;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.METADATA_PATH;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.PUBLIC_BASE;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.SECOND_ORG;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.SECOND_SUBJECT;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.quarkiverse.mcp.server.JsonRpcErrorCodes;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(McpOAuthTest.OAuthProfile.class)
class McpOAuthTest {
  private static final int UNSUPPORTED_PROTOCOL_VERSION = -32022;

  @InjectMock CommandProcessor commandProcessor;

  @Test
  void discoveryIsPublicAndUsesTheConfiguredResource() {
    var metadata = given().get(METADATA_PATH).then().statusCode(200).extract().jsonPath();

    assertThat(metadata.getString("resource")).isEqualTo(expectedResource());
    assertThat(metadata.getList("authorization_servers", String.class))
        .containsExactly(AUTH_SERVER);
    assertThat(metadata.getList("bearer_methods_supported", String.class)).contains("header");
  }

  @Test
  void unauthenticatedRequestsAdvertiseAbsoluteMetadataLocation() {
    var response = catalogRequest(1).post(mcpPath());

    assertChallenge(response);
    verifyNoInteractions(commandProcessor);
  }

  @Test
  void validJwtListsToolsWithoutAStoredSession() {
    String credential = firstToken();
    for (int id = 1; id <= 2; id++) {
      var response =
          catalogRequest(id).header("Authorization", "Bearer " + credential).post(mcpPath());
      assertResponseComplete(response, id);
      assertThat(response.header("Mcp-Session-Id")).isNull();
      assertThat(response.jsonPath().getList("result.tools.name", String.class))
          .contains("findKeyspaces", "findEmbeddingProviders", "findOne", "insertOne");
    }
  }

  @Test
  void validJwtDoesNotAdmitAnOlderProtocol() {
    // OAuth admits the caller; it does not widen the supported protocol revisions.
    var response =
        given()
            .contentType("application/json")
            .accept("application/json, text/event-stream")
            .header("Authorization", "Bearer " + firstToken())
            .header("MCP-Protocol-Version", "2025-11-25")
            .header("Mcp-Method", "initialize")
            .body(
                Map.of(
                    "jsonrpc",
                    "2.0",
                    "id",
                    1,
                    "method",
                    "initialize",
                    "params",
                    Map.of(
                        "protocolVersion", "2025-11-25",
                        "capabilities", Map.of(),
                        "clientInfo", Map.of("name", "oauth-compatibility-test", "version", "1"))))
            .post(mcpPath());

    var error = assertRpcError(response, 400, UNSUPPORTED_PROTOCOL_VERSION);
    assertThat(error.getJsonObject("data").getString("requested")).isEqualTo("2025-11-25");
    assertThat(error.getJsonObject("data").getJsonArray("supported").getList())
        .containsExactly(CURRENT_PROTOCOL_VERSION);
    assertThat(response.header("Mcp-Session-Id")).isNull();
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @EnumSource(McpOAuthTestSupport.InvalidToken.class)
  void invalidBearerTokensCannotReachTheTool(McpOAuthTestSupport.InvalidToken kind) {
    var response =
        providerRequest(1)
            .header(
                "Authorization",
                "Bearer " + McpOAuthTestSupport.invalidToken(kind, expectedResource()))
            .post(mcpPath());

    assertChallenge(response);
    verifyNoInteractions(commandProcessor);
  }

  @Test
  void invalidBearerDoesNotFallBackToAnExistingLegacyToken() {
    var response =
        providerRequest(1)
            .header("Authorization", "Bearer malformed-access-token")
            .header("Token", "legacy-credential")
            .post(mcpPath());

    assertChallenge(response);
    assertThat(response.jsonPath().getInt("error.code"))
        .isEqualTo(JsonRpcErrorCodes.SECURITY_ERROR);
    assertThat(response.asString())
        .doesNotContain(
            "MISSING_AUTHENTICATION_TOKEN", "legacy-credential", "malformed-access-token");
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void legacyTokenHeadersCannotAuthenticateOAuthOnlyMcp(String header) {
    var response = catalogRequest(1).header(header, "legacy-credential").post(mcpPath());

    assertChallenge(response);
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void commandContextReceivesTheVerifiedTokenRatherThanItsSubjectOrLegacyHeaders(String header) {
    String credential = firstToken();
    var observed = new AtomicReference<String>();
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              observed.set(context.requestContext().authToken());
              return Uni.createFrom()
                  .item(CommandResult.statusOnlyBuilder(context.requestTracing()).build());
            });

    var request = providerRequest(1).header("Authorization", "Bearer " + credential);
    if (header != null) {
      request.header(header, "legacy-credential");
    }
    var response = request.post(mcpPath());

    assertResponseComplete(response, 1);
    assertThat(observed.get())
        .isEqualTo(credential)
        .isNotEqualTo(FIRST_SUBJECT)
        .isNotEqualTo("legacy-credential");
    assertThat(response.asString()).doesNotContain(credential, "legacy-credential");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void legacyCredentialsCannotOpenTheSseTransport(String header) {
    var response =
        given()
            // Bound the read if a regression incorrectly admits this streaming request.
            .config(config().httpClient(httpClientConfig().setParam("http.socket.timeout", 5000)))
            .header(header, "legacy-credential")
            .accept("text/event-stream")
            .get(mcpPath() + "/sse");

    assertChallenge(response);
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void legacyCredentialsCannotPostToTheMessageTransport(String header) {
    var response =
        given()
            .header(header, "legacy-credential")
            .contentType("application/json")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post(mcpPath() + "/messages/synthetic-session-id");

    assertChallenge(response);
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void ordinaryRestCallsRetainLegacyAuthentication(String header) {
    var observed = new AtomicReference<String>();
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              observed.set(context.requestContext().authToken());
              return Uni.createFrom()
                  .item(CommandResult.statusOnlyBuilder(context.requestTracing()).build());
            });
    var response =
        given()
            .contentType("application/json")
            .header(header, "legacy-credential")
            .body(Map.of("findEmbeddingProviders", Map.of()))
            .post("/v1");

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    assertThat(observed.get()).isEqualTo("legacy-credential");
    assertThat(response.asString()).doesNotContain("legacy-credential");
  }

  @Test
  void ordinaryRestCallsDoNotAcceptBearerOnlyAuthentication() {
    var response =
        given()
            .contentType("application/json")
            .header("Authorization", "Bearer " + firstToken())
            .body(Map.of("findEmbeddingProviders", Map.of()))
            .post("/v1");

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    verifyNoInteractions(commandProcessor);
  }

  @Test
  void overlappingJwtRequestsKeepSeparateCredentialsWhenCompletedInReverseOrder() throws Exception {
    String firstToken = firstToken();
    String secondToken = token(SECOND_SUBJECT, SECOND_ORG, "write-role", expectedResource());
    var firstCompletion = new CompletableFuture<Integer>();
    var secondCompletion = new CompletableFuture<Integer>();
    var completions = Map.of(firstToken, firstCompletion, secondToken, secondCompletion);
    var started = new LinkedBlockingQueue<RequestContext>();
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              return Uni.createFrom()
                  .<Integer>emitter(
                      emitter -> {
                        started.add(context.requestContext());
                        var completion = completions.get(context.requestContext().authToken());
                        if (completion == null) {
                          emitter.fail(
                              new IllegalStateException("Expected the encoded access token"));
                        } else {
                          completion.thenAccept(emitter::complete);
                        }
                      })
                  .map(
                      value ->
                          CommandResult.statusOnlyBuilder(context.requestTracing())
                              .addStatus(CommandStatus.OK, value)
                              .build());
            });

    try (var clients = Executors.newFixedThreadPool(2);
        var worker = Executors.newSingleThreadExecutor()) {
      worker.submit(() -> {}).get();
      try {
        var firstResponse =
            clients.submit(
                () ->
                    providerRequest(1)
                        .header("Authorization", "Bearer " + firstToken)
                        .post(mcpPath()));
        var firstContext = started.poll(10, TimeUnit.SECONDS);
        assertThat(firstContext).isNotNull();
        var secondResponse =
            clients.submit(
                () ->
                    providerRequest(2)
                        .header("Authorization", "Bearer " + secondToken)
                        .post(mcpPath()));
        var secondContext = started.poll(10, TimeUnit.SECONDS);
        assertThat(secondContext).isNotNull();

        assertThat(firstContext).isNotSameAs(secondContext);
        assertThat(firstContext.authToken()).isEqualTo(firstToken);
        assertThat(secondContext.authToken()).isEqualTo(secondToken);
        assertThat(firstContext.requestId()).isNotEqualTo(secondContext.requestId());
        worker.submit(() -> secondCompletion.complete(2)).get(10, TimeUnit.SECONDS);
        assertThat(
                assertResponseComplete(secondResponse.get(10, TimeUnit.SECONDS), 2)
                    .getJsonObject("_meta")
                    .getJsonObject("status")
                    .getInteger("ok"))
            .isEqualTo(2);
        assertThat(firstResponse.isDone()).isFalse();
        worker.submit(() -> firstCompletion.complete(1)).get(10, TimeUnit.SECONDS);
        assertThat(
                assertResponseComplete(firstResponse.get(10, TimeUnit.SECONDS), 1)
                    .getJsonObject("_meta")
                    .getJsonObject("status")
                    .getInteger("ok"))
            .isEqualTo(1);
      } finally {
        firstCompletion.complete(1);
        secondCompletion.complete(2);
      }
    }
  }

  protected String expectedResource() {
    return PUBLIC_BASE + "/v1/mcp";
  }

  protected String expectedMetadataUrl() {
    return PUBLIC_BASE + METADATA_PATH;
  }

  private String firstToken() {
    return token(FIRST_SUBJECT, FIRST_ORG, "read-role", expectedResource());
  }

  private String mcpPath() {
    return ConfigProvider.getConfig().getValue("quarkus.mcp.server.http.root-path", String.class);
  }

  private RequestSpecification catalogRequest(int id) {
    return request("tools/list").body(rpcPayload(id, "tools/list", Map.of()));
  }

  private RequestSpecification providerRequest(int id) {
    return request("tools/call")
        .header("Mcp-Name", "findEmbeddingProviders")
        .body(McpHttpTestSupport.toolPayload(id, "findEmbeddingProviders", Map.of()));
  }

  private void assertChallenge(Response response) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    assertThat(response.header("WWW-Authenticate"))
        .as("HTTP headers: %s; body: %s", response.headers(), response.asString())
        .contains("Bearer", "resource_metadata=\"" + expectedMetadataUrl() + "\"");
  }

  public static class OAuthProfile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "stargate.feature.flags.mcp",
          "true",
          "stargate.auth.bearer.enabled",
          "true",
          "stargate.mcp.auth.server-url",
          AUTH_SERVER,
          "stargate.mcp.auth.public-base-url",
          PUBLIC_BASE,
          "mp.jwt.verify.issuer",
          AUTH_SERVER,
          "mp.jwt.verify.audiences",
          PUBLIC_BASE + "/v1/mcp",
          "mp.jwt.verify.publickey.location",
          "mcp-oauth-public-key.pem");
    }
  }
}
