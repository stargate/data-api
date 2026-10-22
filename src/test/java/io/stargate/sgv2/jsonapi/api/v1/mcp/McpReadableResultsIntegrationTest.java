package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Checks readable results from the packaged endpoint without a client translating the response. */
@QuarkusIntegrationTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class McpReadableResultsIntegrationTest {
  private static final String PROTOCOL_VERSION = "2026-07-28";
  private static final String TOOL_NAME = CommandName.Names.FIND_RERANKING_PROVIDERS;

  @Test
  void successfulToolTextMatchesTheHttpResponse() {
    // Provider discovery uses packaged configuration and does not need a database or provider call.
    var result = callTool(true);
    assertThat(result.getBoolean("isError")).isFalse();

    var httpResponse =
        given()
            .contentType(ContentType.JSON)
            .header("Token", "readable-result-test-credential")
            .header(ApiFeature.RERANKING.httpHeaderName(), "true")
            .body(Map.of(TOOL_NAME, Map.of()))
            .post("/v1")
            .then()
            .statusCode(200)
            .extract()
            .asString();

    assertThat(readableResult(result)).isEqualTo(new JsonObject(httpResponse));
    assertThat(result.getJsonObject("_meta").getJsonObject("status"))
        .isEqualTo(new JsonObject(httpResponse).getJsonObject("status"));
  }

  @Test
  void failedToolReturnsReadableErrorsInACompleteResult() {
    var result = callTool(false);
    assertThat(result.getBoolean("isError")).isTrue();
    var errors = result.getJsonObject("structuredContent").getJsonArray("errors");
    assertThat(errors.getJsonObject(0).getString("errorCode"))
        .isEqualTo(SchemaException.Code.MCP_FEATURE_NOT_ENABLED.name());
    assertThat(readableResult(result)).isEqualTo(result.getJsonObject("structuredContent"));
  }

  private static JsonObject readableResult(JsonObject result) {
    var content = result.getJsonArray("content");
    assertThat(content).hasSize(1);
    assertThat(content.getJsonObject(0).getString("type")).isEqualTo("text");
    return new JsonObject(content.getJsonObject(0).getString("text"));
  }

  private JsonObject callTool(boolean mcpEnabled) {
    var response =
        given()
            .contentType(ContentType.JSON)
            .accept("application/json, text/event-stream")
            // Existing token fixture. OAuth admission is tracked separately in #2642.
            .header("Token", "readable-result-test-credential")
            .header(ApiFeature.MCP.httpHeaderName(), mcpEnabled)
            .header(ApiFeature.RERANKING.httpHeaderName(), "true")
            .header("MCP-Protocol-Version", PROTOCOL_VERSION)
            .header("Mcp-Method", "tools/call")
            .header("Mcp-Name", TOOL_NAME)
            .body(
                Map.of(
                    "jsonrpc",
                    "2.0",
                    "id",
                    1,
                    "method",
                    "tools/call",
                    "params",
                    Map.of(
                        "name", TOOL_NAME,
                        "arguments", Map.of(),
                        "_meta",
                            Map.of(
                                "io.modelcontextprotocol/protocolVersion",
                                PROTOCOL_VERSION,
                                "io.modelcontextprotocol/clientCapabilities",
                                Map.of()))))
            .post("/v1/mcp");
    assertThat(response.statusCode()).as(response.asString()).isEqualTo(200);
    var body = new JsonObject(response.asString());
    assertThat(body.getInteger("id")).isEqualTo(1);
    assertThat(body.containsKey("error")).isFalse();
    var result = body.getJsonObject("result");
    assertThat(result.getString("resultType")).isEqualTo("complete");
    return result;
  }
}
