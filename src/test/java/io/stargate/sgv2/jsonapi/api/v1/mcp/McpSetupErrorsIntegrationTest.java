package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkiverse.mcp.server.JsonRpcErrorCodes;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies application setup errors without changing authentication or framework error handling.
 */
@QuarkusIntegrationTest
@WithTestResource(value = DseTestResource.class)
class McpSetupErrorsIntegrationTest extends McpIntegrationTestBase {
  private static final String PROTOCOL_VERSION = "2026-07-28";

  @BeforeAll
  void createTestKeyspace() {
    createKeyspace(keyspaceName);
  }

  @AfterAll
  void dropTestKeyspace() {
    dropKeyspace(keyspaceName);
  }

  @Test
  void unknownKeyspaceReturnsAnApplicationErrorThenAValidCallSucceeds() {
    var response =
        callTool(
            1, CommandName.Names.FIND_COLLECTIONS, Map.of("keyspace", keyspaceName + "_missing"));

    assertApplicationError(response, 1, SchemaException.Code.UNKNOWN_KEYSPACE);
    assertProviderDiscoverySucceeds(2);
  }

  @Test
  void unknownCollectionReturnsAnApplicationErrorThenAValidCallSucceeds() {
    var response =
        callTool(
            3,
            CommandName.Names.COUNT_DOCUMENTS,
            Map.of("keyspace", keyspaceName, "collection", collectionName));

    assertApplicationError(response, 3, SchemaException.Code.UNKNOWN_COLLECTION_OR_TABLE);
    assertProviderDiscoverySucceeds(4);
  }

  @Test
  void missingTokenRemainsAnHttpDenialThenAValidCallSucceeds() {
    var response =
        request("tools/call", CommandName.Names.FIND_EMBEDDING_PROVIDERS, false)
            .body(toolPayload(5, CommandName.Names.FIND_EMBEDDING_PROVIDERS, Map.of()))
            .post(MCP_PATH);

    assertThat(response.statusCode()).as(response.asString()).isEqualTo(401);
    var body = new JsonObject(response.asString());
    assertThat(body.containsKey("result")).isFalse();
    assertThat(body.getJsonArray("errors").getJsonObject(0).getString("errorCode"))
        .isEqualTo(APISecurityException.Code.MISSING_AUTHENTICATION_TOKEN.name());
    assertProviderDiscoverySucceeds(6);
  }

  @Test
  void malformedJsonRemainsAProtocolError() {
    var response =
        request("tools/call", CommandName.Names.FIND_EMBEDDING_PROVIDERS, true)
            .body("{invalid-json")
            .post(MCP_PATH);

    // Quarkiverse 2.0.3 reports parse errors in a JSON-RPC envelope with HTTP 200.
    assertProtocolError(response, null, JsonRpcErrorCodes.PARSE_ERROR);
    assertProviderDiscoverySucceeds(7);
  }

  @Test
  void unknownMethodAndToolRemainProtocolErrors() {
    String method = "tools/unknownMethod";
    var response = request(method, null, true).body(rpcPayload(8, method, Map.of())).post(MCP_PATH);

    assertProtocolError(response, 8, JsonRpcErrorCodes.METHOD_NOT_FOUND);
    assertProtocolError(callTool(9, "unknownTool", Map.of()), 9, JsonRpcErrorCodes.INVALID_PARAMS);
    assertProviderDiscoverySucceeds(10);
  }

  @Test
  void missingArgumentRemainsAFrameworkToolError() {
    var result = completeToolResult(callTool(11, CommandName.Names.FIND_COLLECTIONS, Map.of()), 11);

    // The framework's default input-validation strategy returns a plain tool error.
    assertThat(result.getBoolean("isError")).isTrue();
    assertThat(result.getValue("structuredContent")).isNull();
    var content = result.getJsonArray("content");
    assertThat(content).hasSize(1);
    assertThat(content.getJsonObject(0).getString("type")).isEqualTo("text");
    assertThat(content.getJsonObject(0).getString("text"))
        .isEqualTo("Missing required argument: keyspace");
    assertProviderDiscoverySucceeds(12);
  }

  private void assertProviderDiscoverySucceeds(int id) {
    var result =
        completeToolResult(callTool(id, CommandName.Names.FIND_EMBEDDING_PROVIDERS, Map.of()), id);
    assertThat(result.getBoolean("isError")).isFalse();
    var status = result.getJsonObject("_meta").getJsonObject("status");
    assertThat(status.getJsonObject("embeddingProviders")).isNotNull();
    assertThat(readableResult(result)).isEqualTo(new JsonObject().put("status", status));
  }

  private static void assertApplicationError(Response response, int id, SchemaException.Code code) {
    var result = completeToolResult(response, id);
    assertThat(result.getBoolean("isError")).isTrue();
    var structured = result.getJsonObject("structuredContent");
    var errors = structured.getJsonArray("errors");
    assertThat(errors).hasSize(1);
    assertThat(errors.getJsonObject(0).getString("errorCode")).isEqualTo(code.name());
    assertThat(errors.getJsonObject(0).getString("message")).isNotBlank();
    assertThat(readableResult(result)).isEqualTo(structured);
  }

  private static JsonObject readableResult(JsonObject result) {
    var content = result.getJsonArray("content");
    assertThat(content).hasSize(1);
    assertThat(content.getJsonObject(0).getString("type")).isEqualTo("text");
    return new JsonObject(content.getJsonObject(0).getString("text"));
  }

  private static JsonObject completeToolResult(Response response, int id) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    var body = new JsonObject(response.asString());
    assertThat(body.getInteger("id")).isEqualTo(id);
    assertThat(body.containsKey("error")).isFalse();
    var result = body.getJsonObject("result");
    assertThat(result.getString("resultType")).isEqualTo("complete");
    return result;
  }

  private static void assertProtocolError(Response response, Integer id, int errorCode) {
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    var body = new JsonObject(response.asString());
    assertThat(body.getInteger("id")).isEqualTo(id);
    assertThat(body.containsKey("result")).isFalse();
    assertThat(body.getJsonObject("error").getInteger("code")).isEqualTo(errorCode);
  }

  private Response callTool(int id, String tool, Map<String, Object> arguments) {
    return request("tools/call", tool, true).body(toolPayload(id, tool, arguments)).post(MCP_PATH);
  }

  private RequestSpecification request(String method, String tool, boolean authenticated) {
    var request =
        given()
            .contentType(ContentType.JSON)
            .accept("application/json, text/event-stream")
            .header(ApiFeature.MCP.httpHeaderName(), "true")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION)
            .header("Mcp-Method", method);
    if (tool != null) {
      request.header("Mcp-Name", tool);
    }
    if (authenticated) {
      for (var header : authHeaders()) {
        request.header(header.getKey(), header.getValue());
      }
    }
    return request;
  }

  private static Map<String, Object> toolPayload(
      int id, String tool, Map<String, Object> arguments) {
    return rpcPayload(id, "tools/call", Map.of("name", tool, "arguments", arguments));
  }

  private static Map<String, Object> rpcPayload(int id, String method, Map<String, Object> params) {
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
}
