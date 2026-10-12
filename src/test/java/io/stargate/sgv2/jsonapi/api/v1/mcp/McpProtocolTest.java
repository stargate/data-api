package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Exercises the HTTP wire contract without a client that might initialize a session for us. */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpProtocolTest {
  private static final String PROTOCOL_VERSION = "2026-07-28";

  @Test
  void listsToolsWithoutInitializationOrSession() {
    // Each POST is independent. clientInfo, discovery and initialization are deliberately absent.
    for (int id = 1; id <= 2; id++) {
      Response response = request("tools/list", PROTOCOL_VERSION, id);
      assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
      assertThat(response.header("Mcp-Session-Id")).isNull();
      assertThat(response.jsonPath().getInt("id")).isEqualTo(id);
      assertThat(response.jsonPath().getList("result.tools.name")).isNotEmpty();
      assertThat(response.jsonPath().getString("result.resultType")).isEqualTo("complete");
    }
  }

  private Response request(String method, String version, int id) {
    return given()
        .contentType(ContentType.JSON)
        .accept("application/json, text/event-stream")
        // Existing header identity fixture only. OAuth admission is tracked separately in #2642.
        .header("Token", "protocol-test-credential")
        .header("MCP-Protocol-Version", version)
        .header("Mcp-Method", method)
        .body(
            Map.of(
                "jsonrpc",
                "2.0",
                "id",
                id,
                "method",
                method,
                "params",
                Map.of(
                    "_meta",
                    Map.of(
                        "io.modelcontextprotocol/protocolVersion",
                        version,
                        "io.modelcontextprotocol/clientCapabilities",
                        Map.of()))))
        .post("/v1/mcp");
  }
}
