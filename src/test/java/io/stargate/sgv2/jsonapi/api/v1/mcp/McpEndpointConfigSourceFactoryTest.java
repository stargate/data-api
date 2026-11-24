package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class McpEndpointConfigSourceFactoryTest {
  @ParameterizedTest
  @CsvSource({
    "/, /v1/mcp, /v1/mcp",
    "/service, /tools, /service/tools",
    "//service, /tools, /service/tools",
    "/service, //tools, /tools",
    "/service, ///tools, /tools",
    "/service, ////tools, /service",
    "/service/, tools/, /service/tools",
    "/service.v1, /tools, /service.v1/tools"
  })
  void excludesOnlyTheNormalizedEndpointAndItsDescendants(
      String httpRoot, String mcpRoot, String expectedEndpoint) {
    var config =
        config(
            Map.of(
                "quarkus.http.root-path", httpRoot,
                "quarkus.mcp.server.http.root-path", mcpRoot));
    assertThat(config.getValue(McpEndpointConfigSourceFactory.ENDPOINT_PATH, String.class))
        .isEqualTo(expectedEndpoint);
    assertThat(config.getValue("quarkus.http.auth.permission.mcp.paths", String.class))
        .isEqualTo(
            String.join(
                ",",
                expectedEndpoint,
                expectedEndpoint + "/",
                expectedEndpoint + "/sse",
                expectedEndpoint + "/sse/",
                expectedEndpoint + "/messages/*"));
    assertThat(config.getValue("quarkus.http.auth.permission.mcp.policy", String.class))
        .isEqualTo("authenticated");
    var excluded =
        Pattern.compile(config.getValue("quarkus.http.access-log.exclude-pattern", String.class));
    assertThat(excluded.matcher(expectedEndpoint).matches()).isTrue();
    assertThat(excluded.matcher(expectedEndpoint + "/messages").matches()).isTrue();
    assertThat(excluded.matcher(expectedEndpoint + "-other").matches()).isFalse();
    assertThat(excluded.matcher("/serviceXv1/tools").matches()).isFalse();
  }

  @Test
  void rootEndpointExcludesAllDescendants() {
    var config = config(Map.of("quarkus.mcp.server.http.root-path", "/"));
    var excluded =
        Pattern.compile(config.getValue("quarkus.http.access-log.exclude-pattern", String.class));
    assertThat(excluded.matcher("/").matches()).isTrue();
    assertThat(excluded.matcher("/messages").matches()).isTrue();
    assertThat(config.getValue("quarkus.http.auth.permission.mcp.paths", String.class))
        .isEqualTo("/,/sse,/sse/,/messages/*");
  }

  @ParameterizedTest
  @CsvSource({
    "/, /, /, ''",
    "/service, /, /service, /service",
    "/, /v1/mcp, /v1/mcp, /v1/mcp",
    "/service, /tools, /service/tools, /service/tools"
  })
  void protectsOnlySdkTransportRoutes(
      String httpRoot, String mcpRoot, String endpoint, String childRoot) {
    var config =
        config(
            Map.of(
                "quarkus.http.root-path", httpRoot,
                "quarkus.mcp.server.http.root-path", mcpRoot));
    var paths = McpEndpointConfigSourceFactory.transportPaths(endpoint);
    assertThat(config.getValue("quarkus.http.auth.permission.mcp.paths", String.class))
        .isEqualTo(paths.permissionPaths())
        .doesNotContain(endpoint.equals("/") ? "/*," : endpoint + "/*");
    assertThat(paths.matches(endpoint)).isTrue();
    assertThat(paths.matches(endpoint.equals("/") ? "/" : endpoint + "/")).isTrue();
    assertThat(paths.matches(childRoot + "/sse")).isTrue();
    assertThat(paths.matches(childRoot + "/sse/")).isTrue();
    assertThat(paths.matches(childRoot + "/messages/session-id")).isTrue();
    for (String path :
        List.of(
            "/v1/collections",
            "/stargate/health",
            childRoot + "/v1/collections",
            childRoot + "/stargate/health",
            childRoot + "/collections",
            childRoot + "/sse-other",
            childRoot + "/messages-other/session-id",
            endpoint + "-other",
            childRoot + "/unknown")) {
      assertThat(paths.matches(path)).as(path).isFalse();
    }
  }

  @Test
  void metadataRemainsPublicUnderTheConfiguredHttpRoot() {
    var config =
        config(
            Map.of(
                "quarkus.http.root-path",
                "/service",
                "quarkus.mcp.server.http.root-path",
                "/tools"));
    String path = "/service/.well-known/oauth-protected-resource";
    assertThat(config.getValue(McpEndpointConfigSourceFactory.METADATA_PATH, String.class))
        .isEqualTo(path);
    assertThat(config.getValue("quarkus.http.auth.permission.mcp-metadata.paths", String.class))
        .isEqualTo(path);
    assertThat(config.getValue("quarkus.http.auth.permission.mcp-metadata.policy", String.class))
        .isEqualTo("permit");
  }

  @Test
  void explicitExclusionsKeepTheirNormalPrecedence() {
    var config =
        config(
            Map.of(
                "quarkus.mcp.server.http.root-path", "/tools",
                "quarkus.http.access-log.exclude-pattern", "/tools(/.*)?|/health"));
    assertThat(config.getValue("quarkus.http.access-log.exclude-pattern", String.class))
        .isEqualTo("/tools(/.*)?|/health");
  }

  private SmallRyeConfig config(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        .withSources(new PropertiesConfigSource(properties, "test", 300))
        .withSources(new McpEndpointConfigSourceFactory())
        .build();
  }
}
