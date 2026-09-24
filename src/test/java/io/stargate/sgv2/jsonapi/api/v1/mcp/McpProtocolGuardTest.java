package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Wire-level tests for the local single-version MCP transport boundary. */
@QuarkusTest
class McpProtocolGuardTest {

  private static final String PATH = "/v1/mcp";

  private static JsonObject request(String method) {
    return new JsonObject()
        .put("jsonrpc", "2.0")
        .put("id", 1)
        .put("method", method)
        .put(
            "params",
            new JsonObject()
                .put(
                    "_meta",
                    new JsonObject()
                        .put("io.modelcontextprotocol/protocolVersion", McpProtocolGuard.VERSION)
                        .put("io.modelcontextprotocol/clientCapabilities", new JsonObject())));
  }

  @Test
  void discoveryAdvertisesOnlyTheSupportedVersion() {
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(request("server/discover").encode())
        .post(PATH)
        .then()
        .statusCode(200)
        .header("Mcp-Session-Id", nullValue())
        .body("result.supportedVersions", contains(McpProtocolGuard.VERSION))
        .body("result.resultType", equalTo("complete"))
        .body("result.capabilities.tools", notNullValue())
        .body("result.ttlMs", equalTo(0))
        .body("result.cacheScope", equalTo("private"))
        .body("result._meta.'io.modelcontextprotocol/serverInfo'.name", notNullValue());
  }

  @Test
  void toolListingUsesModernResultAndNoSession() {
    JsonObject listRequest = request("tools/list");
    listRequest
        .getJsonObject("params")
        .getJsonObject("_meta")
        .put(
            "io.modelcontextprotocol/clientInfo",
            new JsonObject().put("name", "test").put("version", "1"));
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/list")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(listRequest.encode())
        .post(PATH)
        .then()
        .statusCode(200)
        .header("Mcp-Session-Id", nullValue())
        .body("result.resultType", equalTo("complete"))
        .body("result.tools", not(empty()))
        .body(
            "result.tools.find { it.name == 'find' }.inputSchema.properties.filter.type",
            equalTo("object"))
        .body(
            "result.tools.find { it.name == 'find' }.inputSchema.properties.sort.type",
            equalTo("object"))
        .body("result.ttlMs", notNullValue())
        .body("result.cacheScope", notNullValue());
  }

  @Test
  @Disabled(
      "Quarkiverse 2.0.1 requires optional clientInfo; enable when a fixed stable release exists")
  void toolListingAcceptsOmittedOptionalClientInfo() {
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/list")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(request("tools/list").encode())
        .post(PATH)
        .then()
        .statusCode(200)
        .body("result.resultType", equalTo("complete"));
  }

  @Test
  void authenticationPrecedesDiscoveryAndVersionErrors() {
    given()
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(request("server/discover").encode())
        .post(PATH)
        .then()
        .statusCode(401);
  }

  @Test
  void rejectsUntrustedOrigin() {
    given()
        .header("Token", "test")
        .header("Origin", "https://untrusted.example")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(request("server/discover").encode())
        .post(PATH)
        .then()
        .statusCode(403);
  }

  @Test
  void rejectsOlderVersionAndAdvertisesOnlyNewVersion() {
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", "2025-11-25")
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(request("server/discover").encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32022))
        .body("error.data.supported", contains(McpProtocolGuard.VERSION));
  }

  @Test
  void rejectsMissingVersionHeader() {
    given()
        .header("Token", "test")
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(request("server/discover").encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32020))
        .body("$", not(hasKey("id")));
  }

  @Test
  void rejectsMissingRequiredMetaButAllowsOmittedClientInfo() {
    JsonObject invalid = request("server/discover");
    invalid
        .getJsonObject("params")
        .getJsonObject("_meta")
        .remove("io.modelcontextprotocol/clientCapabilities");
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(invalid.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32602));
  }

  @Test
  void rejectsMalformedOptionalClientInfoOnDiscovery() {
    JsonObject invalid = request("server/discover");
    invalid
        .getJsonObject("params")
        .getJsonObject("_meta")
        .put("io.modelcontextprotocol/clientInfo", "not-an-object");
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "server/discover")
        .contentType(ContentType.JSON)
        .body(invalid.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32602));
  }

  @Test
  void rejectsMissingRequiredMetaOnToolListing() {
    JsonObject invalid = request("tools/list");
    invalid.getJsonObject("params").remove("_meta");
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/list")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(invalid.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32602));
  }

  @Test
  void rejectsHeaderAndBodyMismatchesOnToolRequests() {
    JsonObject listRequest = request("tools/list");
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "find")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(listRequest.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32020));

    listRequest
        .getJsonObject("params")
        .getJsonObject("_meta")
        .put("io.modelcontextprotocol/protocolVersion", "2025-11-25");
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/list")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(listRequest.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32020));
  }

  @Test
  void rejectsMissingAndMismatchedToolNameHeader() {
    JsonObject callRequest = request("tools/call");
    callRequest
        .getJsonObject("params")
        .put("name", "findKeyspaces")
        .put("arguments", new JsonObject());
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/call")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(callRequest.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32020));

    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "tools/call")
        .header("Mcp-Name", "find")
        .contentType(ContentType.JSON)
        .header("Accept", "application/json, text/event-stream")
        .body(callRequest.encode())
        .post(PATH)
        .then()
        .statusCode(400)
        .body("error.code", equalTo(-32020));
  }

  @Test
  void closesLegacyHttpMethodsAndRoutes() {
    given().header("Token", "test").get(PATH).then().statusCode(405);
    given().header("Token", "test").delete(PATH).then().statusCode(405);
    given().header("Token", "test").get(PATH + "/sse").then().statusCode(404);
    given().header("Token", "test").post(PATH + "/messages/123").then().statusCode(404);
  }

  @Test
  void rejectsLegacyInitialization() {
    given()
        .header("Token", "test")
        .header("MCP-Protocol-Version", McpProtocolGuard.VERSION)
        .header("Mcp-Method", "initialize")
        .contentType(ContentType.JSON)
        .body(request("initialize").encode())
        .post(PATH)
        .then()
        .statusCode(404)
        .body("error.code", equalTo(-32601));
  }
}
