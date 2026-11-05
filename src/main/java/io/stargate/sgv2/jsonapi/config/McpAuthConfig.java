package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/** Public authorization discovery for the MCP endpoint. */
@ConfigMapping(prefix = "stargate.mcp.auth")
public interface McpAuthConfig {
  Optional<URI> serverUrl();

  Optional<URI> publicBaseUrl();

  Optional<List<String>> scopesSupported();
}
