package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
  @CsvSource({
    "/, /v1/mcp, /v1/mcp",
    "/, v1/mcp, /v1/mcp",
    "/, /v1/mcp/, /v1/mcp",
    "/api, /v1/mcp, /api/v1/mcp",
    "/api/, v1/mcp, /api/v1/mcp",
    "/, /, /"
  })
  void resolvesTheEndpointLikeQuarkusRoutes(String httpRoot, String mcpRoot, String expected) {
    assertThat(McpOriginFilter.endpointPath(httpRoot, mcpRoot)).isEqualTo(expected);
  }

  @Test
  void matchesTheEndpointAndPathsBelowIt() {
    var filter = new McpOriginFilter("/", "/v1/mcp", Optional.empty());
    assertThat(filter.isMcpPath("/v1/mcp")).isTrue();
    assertThat(filter.isMcpPath("/v1/mcp/")).isTrue();
    assertThat(filter.isMcpPath("/v1/mcp/sse")).isTrue();
    // A keyspace whose name starts with "mcp" belongs to the ordinary HTTP API.
    assertThat(filter.isMcpPath("/v1/mcpkeyspace")).isFalse();
    assertThat(filter.isMcpPath("/v1")).isFalse();
    assertThat(filter.isMcpPath(null)).isFalse();
  }
}
