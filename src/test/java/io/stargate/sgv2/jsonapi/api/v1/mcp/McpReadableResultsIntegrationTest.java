package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResponseComplete;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.assertResultReadable;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.request;
import static io.stargate.sgv2.jsonapi.api.v1.mcp.McpHttpTestSupport.toolPayload;
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
    var expected = new JsonObject(httpResponse);

    assertThat(assertResultReadable(result)).isEqualTo(expected);
    assertThat(result.getJsonObject("_meta").getJsonObject("status"))
        .isEqualTo(expected.getJsonObject("status"));
  }

  @Test
  void failedToolReturnsReadableErrorsInACompleteResult() {
    var result = callTool(false);
    assertThat(result.getBoolean("isError")).isTrue();
    var errors = result.getJsonObject("structuredContent").getJsonArray("errors");
    assertThat(errors.getJsonObject(0).getString("errorCode"))
        .isEqualTo(SchemaException.Code.MCP_FEATURE_NOT_ENABLED.name());
    assertThat(assertResultReadable(result)).isEqualTo(result.getJsonObject("structuredContent"));
  }

  private JsonObject callTool(boolean mcpEnabled) {
    var response =
        request("tools/call")
            .header("Token", "readable-result-test-credential")
            .header(ApiFeature.MCP.httpHeaderName(), mcpEnabled)
            .header(ApiFeature.RERANKING.httpHeaderName(), "true")
            .header("Mcp-Name", TOOL_NAME)
            .body(toolPayload(1, TOOL_NAME, Map.of()))
            .post("/v1/mcp");
    return assertResponseComplete(response, 1);
  }
}
