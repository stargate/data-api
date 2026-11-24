package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Covers Origin configuration and path matching without starting the application. */
class McpOriginFilterTest {

  @Test
  void acceptsExactBrowserOrigins() {
    var origins = List.of("https://app.example.com", "http://localhost:6274", "http://[::1]:8080");
    assertThat(McpOriginFilter.approvedOrigins(origins))
        .containsExactlyInAnyOrderElementsOf(origins);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "*",
        "null",
        "app.example.com",
        "https://app.example.com/",
        "https://app.example.com/path",
        "https://app.example.com?query",
        "https://app.example.com#fragment",
        "https://user@app.example.com",
        "https://APP.example.com",
        "HTTPS://app.example.com",
        "https://app.example.com:443",
        "https://app.example.com:65536",
        "http://app.example.com:80",
        "ftp://app.example.com",
        "https://*.example.com",
        "https://app.example.com, https://other.example.com",
        "not an origin"
      })
  void rejectsConfiguredValuesThatBrowsersNeverSend(String origin) {
    assertThatThrownBy(() -> McpOriginFilter.approvedOrigins(List.of(origin)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(McpOriginFilter.ALLOWED_ORIGINS);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/v1/mcp", "/"})
  void matchesOnlyRegisteredTransportRoutes(String endpoint) {
    var routes = McpEndpointConfigSourceFactory.transportPaths(endpoint);
    var prefix = endpoint.equals("/") ? "" : endpoint;

    assertThat(routes.matches(endpoint)).isTrue();
    for (String suffix : List.of("/", "/sse", "/sse/", "/messages/id", "/messages/id/")) {
      assertThat(routes.matches(prefix + suffix)).as(suffix).isTrue();
    }
    for (String suffix : List.of("/orders", "/messages", "/messages/", "/messages/id/details")) {
      assertThat(routes.matches(prefix + suffix)).as(suffix).isFalse();
    }
    assertThat(routes.matches("/v1/mcpkeyspace")).isFalse();
    assertThat(routes.matches(null)).isFalse();
  }
}
