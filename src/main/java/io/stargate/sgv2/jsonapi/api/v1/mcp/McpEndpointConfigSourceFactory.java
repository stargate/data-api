package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.PropertiesConfigSource;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.spi.ConfigSource;

/** Shares the normalized MCP endpoint with request logging and Quarkus access logging. */
@StaticInitSafe
public class McpEndpointConfigSourceFactory implements ConfigSourceFactory {
  static final String ENDPOINT_PATH = "stargate.jsonapi.mcp.endpoint-path";

  @Override
  public Iterable<ConfigSource> getConfigSources(ConfigSourceContext context) {
    String mcpRoot = context.getValue("quarkus.mcp.server.http.root-path").getValue();
    if (mcpRoot == null) {
      // Quarkus also builds bootstrap configurations before application configuration is loaded.
      return List.of();
    }
    String httpRoot = context.getValue("quarkus.http.root-path").getValue();
    httpRoot = httpRoot == null ? "/" : httpRoot.replace("//", "/");
    httpRoot = httpRoot.endsWith("/") ? httpRoot : httpRoot + "/";
    mcpRoot = mcpRoot.startsWith("/") ? mcpRoot.substring(1) : mcpRoot;
    // Match Quarkus route normalization, including its treatment of repeated leading slashes.
    URI mcpUri = URI.create(mcpRoot.replace("//", "/")).normalize();
    if (mcpUri.getPath().isEmpty()) {
      mcpUri = URI.create("");
    }
    String endpointPath = URI.create(httpRoot).resolve(mcpUri).normalize().getPath();
    if (endpointPath.endsWith("/") && endpointPath.length() > 1) {
      endpointPath = endpointPath.substring(0, endpointPath.length() - 1);
    }
    String excludePattern =
        Pattern.quote(endpointPath) + (endpointPath.equals("/") ? ".*" : "(/.*)?");
    // A config source is needed because Quarkus creates its access logger before CDI startup.
    // The default source ordinal leaves explicit operator exclusions at their normal precedence.
    return List.of(
        new PropertiesConfigSource(
            Map.of(
                ENDPOINT_PATH,
                endpointPath,
                "quarkus.http.access-log.exclude-pattern",
                excludePattern),
            "MCP endpoint"));
  }
}
