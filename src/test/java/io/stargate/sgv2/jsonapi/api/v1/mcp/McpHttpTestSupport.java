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
  static final String CURRENT_PROTOCOL_VERSION = "2026-07-28";
  static final String SERVER_INFO = "io.modelcontextprotocol/serverInfo";

  private McpHttpTestSupport() {}

  /** A POST with the Accept and Content-Type headers but none of the MCP request headers. */
  static RequestSpecification bareRequest() {
    return given().contentType(ContentType.JSON).accept("application/json, text/event-stream");
  }

  static RequestSpecification request(String method) {
    return bareRequest()
        .header("MCP-Protocol-Version", CURRENT_PROTOCOL_VERSION)
        .header("Mcp-Method", method);
  }

  static Map<String, Object> toolPayload(int id, String tool, Map<String, Object> arguments) {
    return rpcPayload(id, "tools/call", Map.of("name", tool, "arguments", arguments));
  }

  static Map<String, Object> rpcPayload(int id, String method, Map<String, Object> params) {
    return rpcPayload(id, method, params, requestMeta(CURRENT_PROTOCOL_VERSION));
  }

  /** Builds a request with the given {@code _meta}, or without one when {@code meta} is null. */
  static Map<String, Object> rpcPayload(
      int id, String method, Map<String, Object> params, Map<String, Object> meta) {
    var requestParams = new HashMap<>(params);
    if (meta != null) {
      requestParams.put("_meta", meta);
    }
    return Map.of("jsonrpc", "2.0", "id", id, "method", method, "params", requestParams);
  }

  /** A notification has no id, so the server must not answer it with a result. */
  static Map<String, Object> notificationPayload(String method) {
    return Map.of(
        "jsonrpc",
        "2.0",
        "method",
        method,
        "params",
        Map.of("_meta", requestMeta(CURRENT_PROTOCOL_VERSION)));
  }

  /** The required per-request fields, with clientInfo omitted because it is optional. */
  static Map<String, Object> requestMeta(String protocolVersion) {
    return Map.of(
        "io.modelcontextprotocol/protocolVersion",
        protocolVersion,
        "io.modelcontextprotocol/clientCapabilities",
        Map.of());
  }

  static JsonObject assertResponseComplete(Response response, int id) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    var body = new JsonObject(response.asString());
    assertThat(body.getInteger("id")).isEqualTo(id);
    assertThat(body.containsKey("error")).isFalse();
    var result = body.getJsonObject("result");
    assertThat(result.getString("resultType")).isEqualTo("complete");
    return result;
  }

  /** Asserts a JSON-RPC error envelope with the given HTTP status and error code. */
  static JsonObject assertRpcError(Response response, int httpStatus, int code) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(httpStatus);
    var body = new JsonObject(response.asString());
    assertThat(body.containsKey("result")).isFalse();
    var error = body.getJsonObject("error");
    assertThat(error.getInteger("code")).as(error.encode()).isEqualTo(code);
    return error;
  }

  static JsonObject assertResultReadable(JsonObject result) {
    var content = result.getJsonArray("content");
    assertThat(content).hasSize(1);
    assertThat(content.getJsonObject(0).getString("type")).isEqualTo("text");
    return new JsonObject(content.getJsonObject(0).getString("text"));
  }
}
