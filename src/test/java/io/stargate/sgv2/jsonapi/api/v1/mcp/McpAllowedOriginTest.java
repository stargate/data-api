package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(McpAllowedOriginTest.AllowedOriginProfile.class)
class McpAllowedOriginTest {

  public static class AllowedOriginProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.mcp.allowed-origins", "https://trusted.example");
    }
  }

  @Test
  void permitsExplicitlyAllowedOrigin() {
    JsonObject request =
        new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", 1)
            .put("method", "server/discover")
            .put(
                "params",
                new JsonObject()
                    .put(
                        "_meta",
                        new JsonObject()
                            .put(
                                "io.modelcontextprotocol/protocolVersion", McpProtocolGuard.VERSION)
                            .put("io.modelcontextprotocol/clientCapabilities", new JsonObject())));
    given()
        .header("Token", "test")
        .header("Origin", "https://trusted.example")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(request.encode())
        .post("/v1/mcp")
        .then()
        .statusCode(200)
        .body("result.supportedVersions", contains(McpProtocolGuard.VERSION));
  }
}
