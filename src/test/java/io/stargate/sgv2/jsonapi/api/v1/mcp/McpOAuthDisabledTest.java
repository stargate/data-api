package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(McpOAuthDisabledTest.DisabledProfile.class)
class McpOAuthDisabledTest {
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
      return overrides;
    }
  }
}
