package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
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
