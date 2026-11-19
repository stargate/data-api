package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(McpOAuthDisabledTest.DisabledProfile.class)
class McpOAuthDisabledTest {
  @InjectMock CommandProcessor commandProcessor;

  @Test
  void legacyDatabaseDenialRemainsAToolError() {
    var denied =
        APISecurityException.Code.UNAUTHORIZED_ACCESS.get(
            Map.of("keyspace", "test", "table", "items"));
    when(commandProcessor.processCommand(any(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
                        .addThrowable(denied)
                        .build()));

    var response =
        request("tools/call")
            .header("Mcp-Name", "findEmbeddingProviders")
            .body(McpHttpTestSupport.toolPayload(1, "findEmbeddingProviders", Map.of()))
            .header("Token", "legacy-credential")
            .post("/v1/mcp");
    assertThat(assertResponseComplete(response, 1).getBoolean("isError")).isTrue();
    assertThat(response.header("WWW-Authenticate")).isNull();
  }

  @Test
  void disabledBearerAuthenticationDoesNotAdvertiseOAuthMetadata() {
    given().get(McpOAuthTestSupport.METADATA_PATH).then().statusCode(404);
  }

  @Test
  void aValidBearerTokenDoesNotEnableAuthenticationWithoutOptingIn() {
    String credential =
        McpOAuthTestSupport.token(
            McpOAuthTestSupport.FIRST_SUBJECT,
            McpOAuthTestSupport.FIRST_ORG,
            "read-role",
            McpOAuthTestSupport.PUBLIC_BASE + "/v1/mcp");
    request("tools/list")
        .header("Authorization", "Bearer " + credential)
        .body(rpcPayload(1, "tools/list", Map.of()))
        .post("/v1/mcp")
        .then()
        .statusCode(401);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "X-Cassandra-Token"})
  void legacyMcpAuthenticationRemainsUsableWhenOAuthIsDisabled(String header) {
    var response =
        request("tools/list")
            .header(header, "legacy-credential")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp");

    assertResponseComplete(response, 1);
  }

  public static class DisabledProfile extends McpOAuthTest.OAuthProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      var overrides = new HashMap<>(super.getConfigOverrides());
      overrides.put("stargate.auth.bearer.enabled", "false");
      overrides.put("quarkus.http.auth.inclusive", "true");
      return overrides;
    }
  }
}
