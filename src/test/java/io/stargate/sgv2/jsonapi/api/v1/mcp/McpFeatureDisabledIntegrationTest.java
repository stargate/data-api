package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static io.stargate.sgv2.jsonapi.api.v1.util.DataApiCommandSenders.assertGeneralCommand;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import io.quarkiverse.mcp.server.test.McpAssured;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration test to verify MCP feature flag enforcement. When MCP feature is disabled, MCP tool
 * invocations should fail. But with a per-request header override, they should succeed.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = McpFeatureDisabledIntegrationTest.TestResource.class,
    restrictToAnnotatedClass = true)
public class McpFeatureDisabledIntegrationTest extends McpIntegrationTestBase {

  /** Custom test resource that disables the MCP feature flag. */
  public static class TestResource extends DseTestResource {
    public TestResource() {}

    @Override
    public String getFeatureFlagMcp() {
      // return BLANK String to leave feature "undefined" (disabled unless per-request header
      // override)
      // ("false" would be "disabled" for all tests, regardless of headers)
      return " ";
    }
  }

  @Test
  public void restCommandsStillWorkWhenMcpDisabled() {
    // REST API commands should still function even when MCP feature flag is disabled
    assertGeneralCommand().postFindEmbeddingProviders().wasSuccessful();
  }

  @Test
  public void failToolCallWithMCPDisabled() {
    callToolAndAssert(
        CommandName.Names.FIND_KEYSPACES,
        Map.of(),
        assertErrorOnly(
            errorsArray -> {
              assertThat(errorsArray).hasSize(1);
              assertEquals(
                  errorsArray.getJsonObject(0).getString("errorCode"),
                  SchemaException.Code.MCP_FEATURE_NOT_ENABLED.name());
            }));
  }

  @Test
  public void okToolCallWithHeaderEnableMCP() {
    var mcpClientWithHeaderEnable =
        McpAssured.newStreamableClient()
            .setBaseUri(URI.create(MCP_HOSTNAME + getTestPort()))
            .setMcpPath(MCP_PATH)
            .setAdditionalHeaders(msg -> authHeaders().add(ApiFeature.MCP.httpHeaderName(), "true"))
            .build()
            .connect();

    mcpClientWithHeaderEnable
        .when()
        .toolsCall(
            CommandName.Names.CREATE_KEYSPACE, Map.of("name", keyspaceName), assertStatusOnlyOk())
        .thenAssertResults();
  }
}
