package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpOriginPolicyTest.assertRejected;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** With no approved Origins configured, every browser request to the MCP endpoint is rejected. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpOriginDefaultPolicyTest {

  @ParameterizedTest
  // Local Origins are rejected too. The default does not trust localhost.
  @ValueSource(strings = {"https://client.example.com", "http://localhost:6274", "null"})
  void rejectsEveryBrowserOrigin(String origin) {
    assertRejected(
        request("tools/list")
            .header("Token", "origin-test-credential")
            .header("Origin", origin)
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp"));
  }

  @Test
  void rejectsEveryPreflight() {
    assertRejected(
        given()
            .header("Origin", "http://localhost:6274")
            .header("Access-Control-Request-Method", "POST")
            .options("/v1/mcp"));
  }

  @Test
  void allowsNativeClientsWithoutOrigin() {
    assertResponseComplete(
        request("tools/list")
            .header("Token", "origin-test-credential")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post("/v1/mcp"),
        1);
  }
}
