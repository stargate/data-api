package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.CURRENT_PROTOCOL_VERSION;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.SERVER_INFO;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertRpcError;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.bareRequest;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.notificationPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.requestMeta;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.rpcPayload;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.toolPayload;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exercises the HTTP wire contract without a client that might initialize a session for us.
 *
 * <p>Expected statuses and codes follow the 2026-07-28 Streamable HTTP transport, versioning and
 * caching sections of the MCP specification. No test here executes a tool. Other tests in this JVM
 * reset the shared TenantFactory, so a tool result would depend on test order. Completed tool
 * results are checked over raw HTTP in the MCP integration tests instead.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpProtocolTest {
  private static final String MCP_PATH = "/v1/mcp";
  private static final int HEADER_MISMATCH = -32020;
  private static final int UNSUPPORTED_PROTOCOL_VERSION = -32022;
  private static final int METHOD_NOT_FOUND = -32601;
  private static final int INVALID_PARAMS = -32602;
  private static final int PARSE_ERROR = -32700;

  // Header auth checks only that a token is present. Database requests validate it, but these
  // requests never reach the database.
  private static RequestSpecification authorized(RequestSpecification spec) {
    return spec.header("Token", "protocol-test-credential");
  }

  private static RequestSpecification authorized(String method) {
    return authorized(request(method));
  }

  @Test
  void listsToolsWithoutInitializationOrSession() {
    // Each POST is independent. clientInfo, discovery and initialization are deliberately absent.
    for (int id = 1; id <= 2; id++) {
      var response =
          authorized("tools/list").body(rpcPayload(id, "tools/list", Map.of())).post(MCP_PATH);
      assertResponseComplete(response, id);
      assertThat(response.header("Mcp-Session-Id")).isNull();
      assertThat(response.jsonPath().getList("result.tools.name")).isNotEmpty();
    }
  }

  @Test
  void discoverAdvertisesOnlyTheSupportedRevision() {
    var response =
        authorized("server/discover")
            .body(rpcPayload(1, "server/discover", Map.of()))
            .post(MCP_PATH);

    var result = assertResponseComplete(response, 1);
    assertThat(result.getJsonArray("supportedVersions").getList())
        .containsExactly(CURRENT_PROTOCOL_VERSION);
    assertPrivateAndImmediatelyStale(result);
    assertThat(result.getJsonObject("_meta").getJsonObject(SERVER_INFO).getString("name"))
        .isEqualTo("Stargate DATA API");

    // quarkus-mcp-server 2.1.0.Beta1 always adds "logging", so it is not asserted here.
    var capabilities = result.getJsonObject("capabilities");
    assertThat(capabilities.getJsonObject("tools").containsKey("listChanged")).isFalse();
    assertThat(capabilities.fieldNames())
        .contains("tools")
        .doesNotContain("prompts", "resources", "completions", "extensions", "tasks");
  }

  @Test
  void toolsListCarriesCacheHintsAndServerInfo() {
    var response =
        authorized("tools/list").body(rpcPayload(1, "tools/list", Map.of())).post(MCP_PATH);

    var result = assertResponseComplete(response, 1);
    assertPrivateAndImmediatelyStale(result);
    assertThat(result.getJsonObject("_meta").getJsonObject(SERVER_INFO).getString("name"))
        .isEqualTo("Stargate DATA API");
  }

  @Test
  void toolsListOrderIsStableAcrossRequestsAndPages() {
    var first = listAllToolNames();
    var second = listAllToolNames();

    assertThat(first).isNotEmpty().doesNotHaveDuplicates();
    assertThat(second).containsExactlyElementsOf(first);
  }

  @ParameterizedTest
  @ValueSource(strings = {"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25", "2099-01-01"})
  void rejectsUnsupportedRevisions(String version) {
    var response =
        authorized(bareRequest())
            .header("MCP-Protocol-Version", version)
            .header("Mcp-Method", "tools/list")
            .body(rpcPayload(1, "tools/list", Map.of(), requestMeta(version)))
            .post(MCP_PATH);

    var error = assertRpcError(response, 400, UNSUPPORTED_PROTOCOL_VERSION);
    assertThat(error.getJsonObject("data").getJsonArray("supported").getList())
        .containsExactly(CURRENT_PROTOCOL_VERSION);
    assertThat(error.getJsonObject("data").getString("requested")).isEqualTo(version);
  }

  @Test
  void rejectsMissingProtocolVersionHeader() {
    var response =
        authorized(bareRequest())
            .header("Mcp-Method", "tools/list")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void rejectsProtocolVersionHeaderThatDiffersFromMeta() {
    var response =
        authorized("tools/list")
            .body(rpcPayload(1, "tools/list", Map.of(), requestMeta("2025-11-25")))
            .post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void rejectsMissingMcpMethodHeader() {
    var response =
        authorized(bareRequest())
            .header("MCP-Protocol-Version", CURRENT_PROTOCOL_VERSION)
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void rejectsMcpMethodHeaderThatDiffersFromBody() {
    var response =
        authorized("server/discover").body(rpcPayload(1, "tools/list", Map.of())).post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void rejectsToolCallWithoutMcpName() {
    var response =
        authorized("tools/call").body(toolPayload(1, "findKeyspaces", Map.of())).post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void rejectsMcpNameThatDiffersFromBody() {
    var response =
        authorized("tools/call")
            .header("Mcp-Name", "findCollections")
            .body(toolPayload(1, "findKeyspaces", Map.of()))
            .post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
  }

  @Test
  void acceptsHeaderNamesInAnyCase() {
    var response =
        authorized(bareRequest())
            .header("mcp-protocol-version", CURRENT_PROTOCOL_VERSION)
            .header("MCP-METHOD", "tools/list")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post(MCP_PATH);

    assertResponseComplete(response, 1);
  }

  @Test
  void rejectsRequestWithoutMeta() {
    var response =
        authorized("tools/list").body(rpcPayload(1, "tools/list", Map.of(), null)).post(MCP_PATH);

    assertRpcError(response, 400, INVALID_PARAMS);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "io.modelcontextprotocol/protocolVersion",
        "io.modelcontextprotocol/clientCapabilities"
      })
  void rejectsMetaWithoutRequiredField(String field) {
    var meta = new HashMap<>(requestMeta(CURRENT_PROTOCOL_VERSION));
    meta.remove(field);
    var response =
        authorized("tools/list").body(rpcPayload(1, "tools/list", Map.of(), meta)).post(MCP_PATH);

    assertRpcError(response, 400, INVALID_PARAMS);
  }

  @Test
  void rejectsClientInfoThatIsNotAnObject() {
    var meta = new HashMap<>(requestMeta(CURRENT_PROTOCOL_VERSION));
    meta.put("io.modelcontextprotocol/clientInfo", "protocol-test");
    var response =
        authorized("tools/list").body(rpcPayload(1, "tools/list", Map.of(), meta)).post(MCP_PATH);

    assertRpcError(response, 400, INVALID_PARAMS);
  }

  @Test
  void rejectsInitializeWithoutCreatingSession() {
    var params =
        Map.<String, Object>of(
            "protocolVersion",
            CURRENT_PROTOCOL_VERSION,
            "capabilities",
            Map.of(),
            "clientInfo",
            Map.of("name", "protocol-test", "version", "1"));
    var response =
        authorized("initialize").body(rpcPayload(1, "initialize", params)).post(MCP_PATH);

    assertRpcError(response, 404, METHOD_NOT_FOUND);
    assertThat(response.header("Mcp-Session-Id")).isNull();
  }

  @Test
  void rejectsLegacyInitializeHandshake() {
    // A 2025-06-18 client sends initialize with no MCP-Protocol-Version header or _meta.
    var legacy =
        Map.of(
            "jsonrpc",
            "2.0",
            "id",
            1,
            "method",
            "initialize",
            "params",
            Map.of(
                "protocolVersion",
                "2025-06-18",
                "capabilities",
                Map.of(),
                "clientInfo",
                Map.of("name", "protocol-test", "version", "1")));
    var response = authorized(bareRequest()).body(legacy).post(MCP_PATH);

    assertRpcError(response, 400, HEADER_MISMATCH);
    assertThat(response.header("Mcp-Session-Id")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"ping", "logging/setLevel", "no/such/method"})
  void rejectsRemovedAndUnknownMethods(String method) {
    var params =
        method.equals("logging/setLevel")
            ? Map.<String, Object>of("level", "debug")
            : Map.<String, Object>of();
    var response = authorized(method).body(rpcPayload(1, method, params)).post(MCP_PATH);

    assertRpcError(response, 404, METHOD_NOT_FOUND);
  }

  @ParameterizedTest
  @ValueSource(strings = {"notifications/initialized", "notifications/roots/list_changed"})
  void rejectsRemovedNotifications(String method) {
    var response = authorized(method).body(notificationPayload(method)).post(MCP_PATH);

    assertRpcError(response, 404, METHOD_NOT_FOUND);
  }

  @Test
  void rejectsUnknownTool() {
    var response =
        authorized("tools/call")
            .header("Mcp-Name", "noSuchTool")
            .body(toolPayload(1, "noSuchTool", Map.of()))
            .post(MCP_PATH);

    assertThat(new JsonObject(response.asString()).getJsonObject("error").getInteger("code"))
        .isEqualTo(INVALID_PARAMS);
  }

  @Test
  void reportsMalformedJsonAsParseError() {
    var response = authorized("tools/list").body("{not json").post(MCP_PATH);

    // The HTTP status is not asserted. The framework answers 200, and the transport section does
    // not name a status for a malformed request body.
    var error = new JsonObject(response.asString()).getJsonObject("error");
    assertThat(error.getInteger("code")).isEqualTo(PARSE_ERROR);
  }

  @Test
  void ignoresObsoleteSessionAndResumptionHeaders() {
    var response =
        authorized("tools/list")
            .header("Mcp-Session-Id", "obsolete-session")
            .header("Last-Event-ID", "42")
            .body(rpcPayload(1, "tools/list", Map.of()))
            .post(MCP_PATH);

    assertResponseComplete(response, 1);
    assertThat(response.contentType()).startsWith("application/json");
    assertThat(response.header("Mcp-Session-Id")).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "DELETE"})
  void rejectsNonPostMethodsOnTheEndpoint(String httpMethod) {
    // With and without the version header, as an older client might send either.
    for (var spec :
        List.of(given(), given().header("MCP-Protocol-Version", CURRENT_PROTOCOL_VERSION))) {
      Response response = authorized(spec).request(httpMethod, MCP_PATH);
      assertThat(response.statusCode()).isEqualTo(405);
      assertThat(response.header("Allow")).isEqualTo("POST");
    }
  }

  @Test
  void legacyHttpSseRoutesAreRemoved() {
    assertThat(authorized(given()).get(MCP_PATH + "/sse").statusCode()).isEqualTo(404);
    assertThat(
            authorized(given())
                .contentType("application/json")
                .body("{}")
                .post(MCP_PATH + "/messages/obsolete-session")
                .statusCode())
        .isEqualTo(404);
  }

  private static void assertPrivateAndImmediatelyStale(JsonObject result) {
    // ttlMs must be an integer, not a decimal or string.
    assertThat(result.getValue("ttlMs")).isInstanceOf(Integer.class).isEqualTo(0);
    assertThat(result.getString("cacheScope")).isEqualTo("private");
  }

  private static List<String> listAllToolNames() {
    var names = new ArrayList<String>();
    String cursor = null;
    int id = 1;
    do {
      var params =
          cursor == null ? Map.<String, Object>of() : Map.<String, Object>of("cursor", cursor);
      var response =
          authorized("tools/list").body(rpcPayload(id, "tools/list", params)).post(MCP_PATH);
      var result = assertResponseComplete(response, id++);
      result
          .getJsonArray("tools")
          .forEach(tool -> names.add(((JsonObject) tool).getString("name")));
      cursor = result.getString("nextCursor");
    } while (cursor != null);
    return names;
  }
}
