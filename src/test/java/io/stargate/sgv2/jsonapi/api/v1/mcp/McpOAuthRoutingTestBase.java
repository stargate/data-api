package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.config;
import static io.restassured.RestAssured.given;
import static io.restassured.config.HttpClientConfig.httpClientConfig;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.AUTH_SERVER;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_ORG;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.FIRST_SUBJECT;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.METADATA_PATH;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.PUBLIC_BASE;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOAuthTestSupport.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

abstract class McpOAuthRoutingTestBase {
  static final String BROWSER_ORIGIN = "https://client.example.test";

  @InjectMock CommandProcessor commandProcessor;

  @Test
  void approvedBrowserOriginCanFollowTheChallengeToPublicMetadata() {
    var challenge = catalogRequest(1).header("Origin", BROWSER_ORIGIN).post(mcpPath());
    assertChallenge(challenge);
    assertThat(challenge.header("Access-Control-Allow-Origin")).isEqualTo(BROWSER_ORIGIN);
    assertThat(challenge.header("Access-Control-Expose-Headers")).contains("WWW-Authenticate");

    var response = given().header("Origin", BROWSER_ORIGIN).get(METADATA_PATH);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.header("Access-Control-Allow-Origin")).isEqualTo(BROWSER_ORIGIN);
    assertThat(response.getHeaders().getValues("Vary")).contains("Origin");
    var metadata = response.jsonPath();

    assertThat(metadata.getString("resource")).isEqualTo(expectedResource());
    assertThat(metadata.getList("authorization_servers", String.class))
        .containsExactly(AUTH_SERVER);
    assertThat(metadata.getList("bearer_methods_supported", String.class)).contains("header");
  }

  @Test
  void publicMetadataDoesNotGrantBrowserAccessToOtherOrigins() {
    var response = given().header("Origin", "https://other.example.test").get(METADATA_PATH);
    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.header("Access-Control-Allow-Origin")).isNull();
    assertThat(response.getHeaders().getValues("Vary")).contains("Origin");
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void commandContextReceivesTheVerifiedTokenRatherThanItsSubjectOrLegacyHeaders(String header) {
    String credential = firstToken();
    var observed = captureDatabaseCredential();

    var request = providerRequest(1).header("Authorization", "Bearer " + credential);
    if (header != null) {
      request.header(header, "legacy-credential");
    }
    var response = request.post(mcpPath());

    assertResponseComplete(response, 1);
    assertThat(observed.get()).isEqualTo(credential);
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
    var observed = captureDatabaseCredential();
    var response =
        given()
            .contentType("application/json")
            .header(header, "legacy-credential")
            .body(Map.of("findEmbeddingProviders", Map.of()))
            .post("/v1");

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    assertThat(observed.get()).isEqualTo(expectedRestCredential());
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

  private AtomicReference<String> captureDatabaseCredential() {
    var observed = new AtomicReference<String>();
    when(commandProcessor.processCommand(any(), any()))
        .thenAnswer(
            invocation -> {
              CommandContext<?> context = invocation.getArgument(0);
              observed.set(context.requestContext().authToken());
              return Uni.createFrom()
                  .item(CommandResult.statusOnlyBuilder(context.requestTracing()).build());
            });
    return observed;
  }

  String expectedRestCredential() {
    return "legacy-credential";
  }

  String expectedResource() {
    return PUBLIC_BASE + "/v1/mcp";
  }

  String expectedMetadataUrl() {
    return PUBLIC_BASE + METADATA_PATH;
  }

  String firstToken() {
    return token(FIRST_SUBJECT, FIRST_ORG, "read-role", expectedResource());
  }

  String mcpPath() {
    return ConfigProvider.getConfig().getValue("quarkus.mcp.server.http.root-path", String.class);
  }

  RequestSpecification catalogRequest(int id) {
    return request("tools/list").body(rpcPayload(id, "tools/list", Map.of()));
  }

  RequestSpecification providerRequest(int id) {
    return request("tools/call")
        .header("Mcp-Name", "findEmbeddingProviders")
        .body(McpHttpTestSupport.toolPayload(id, "findEmbeddingProviders", Map.of()));
  }

  void assertChallenge(Response response) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    assertThat(response.header("WWW-Authenticate"))
        .as("HTTP headers: %s; body: %s", response.headers(), response.asString())
        .contains("Bearer", "resource_metadata=\"" + expectedMetadataUrl() + "\"");
  }
}
