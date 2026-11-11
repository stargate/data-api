package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import java.net.URI;
import java.util.List;
import java.util.Optional;

/** Public authorization discovery for the MCP endpoint. */
@ConfigMapping(prefix = "stargate.mcp.auth")
public interface McpAuthConfig {
  Optional<URI> serverUrl();

  /** The one public origin of a deployment that serves a single host. */
  Optional<URI> publicBaseUrl();

  /**
   * The DNS suffix shared by the public hosts of a deployment that serves one host per database,
   * such as {@code .apps.example.com}. Each request host is then its own resource and audience. Set
   * this or {@link #publicBaseUrl()}, not both.
   */
  Optional<String> publicHostSuffix();

  Optional<List<String>> scopesSupported();
}
