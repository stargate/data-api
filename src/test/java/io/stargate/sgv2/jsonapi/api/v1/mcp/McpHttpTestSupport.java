package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

/** Shared wire format for raw HTTP tests that do not initialize an MCP client session. */
final class McpHttpTestSupport {
  private static final String PROTOCOL_VERSION = "2026-07-28";

  private McpHttpTestSupport() {}

  static RequestSpecification request(String method) {
    return given()
        .contentType(ContentType.JSON)
        .accept("application/json, text/event-stream")
        .header("MCP-Protocol-Version", PROTOCOL_VERSION)
        .header("Mcp-Method", method);
  }

  static Map<String, Object> toolPayload(int id, String tool, Map<String, Object> arguments) {
    return rpcPayload(id, "tools/call", Map.of("name", tool, "arguments", arguments));
  }

  static Map<String, Object> rpcPayload(int id, String method, Map<String, Object> params) {
    var requestParams = new HashMap<>(params);
    requestParams.put(
        "_meta",
        Map.of(
            "io.modelcontextprotocol/protocolVersion",
            PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientCapabilities",
            Map.of()));
    return Map.of("jsonrpc", "2.0", "id", id, "method", method, "params", requestParams);
  }

  static JsonObject completeResult(Response response, int id) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    var body = new JsonObject(response.asString());
    assertThat(body.getInteger("id")).isEqualTo(id);
    assertThat(body.containsKey("error")).isFalse();
    var result = body.getJsonObject("result");
    assertThat(result.getString("resultType")).isEqualTo("complete");
    return result;
  }

  static JsonObject readableResult(JsonObject result) {
    var content = result.getJsonArray("content");
    assertThat(content).hasSize(1);
    assertThat(content.getJsonObject(0).getString("type")).isEqualTo("text");
    return new JsonObject(content.getJsonObject(0).getString("text"));
  }
}
