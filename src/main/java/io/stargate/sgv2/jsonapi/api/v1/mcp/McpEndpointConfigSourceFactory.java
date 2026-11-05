package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.PropertiesConfigSource;
import io.stargate.sgv2.jsonapi.api.security.ProtectedResourceMetadataResource;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.spi.ConfigSource;

/** Shares normalized MCP paths with request logging and HTTP authentication. */
@StaticInitSafe
public class McpEndpointConfigSourceFactory implements ConfigSourceFactory {
  public static final String ENDPOINT_PATH = "stargate.jsonapi.mcp.endpoint-path";
  public static final String METADATA_PATH = "stargate.jsonapi.mcp.metadata-path";

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
    String metadataPath =
        URI.create(httpRoot)
            .resolve(ProtectedResourceMetadataResource.PATH.substring(1))
            .normalize()
            .getPath();
    // A config source is needed because Quarkus creates its access logger before CDI startup.
    // The default source ordinal leaves explicit operator exclusions at their normal precedence.
    return List.of(
        new PropertiesConfigSource(
            Map.of(
                ENDPOINT_PATH,
                endpointPath,
                METADATA_PATH,
                metadataPath,
                "quarkus.http.access-log.exclude-pattern",
                excludePattern,
                "quarkus.http.auth.permission.mcp.paths",
                transportPaths(endpointPath).permissionPaths(),
                "quarkus.http.auth.permission.mcp.policy",
                "authenticated",
                "quarkus.http.auth.permission.mcp-metadata.paths",
                metadataPath,
                "quarkus.http.auth.permission.mcp-metadata.policy",
                "permit"),
            "MCP endpoint"));
  }

  /** Mirrors the streamable HTTP, SSE, and message routes registered by MCP SDK 2.0.3. */
  public static TransportPaths transportPaths(String endpointPath) {
    String childRoot = endpointPath.equals("/") ? "" : endpointPath;
    return new TransportPaths(endpointPath, childRoot + "/sse", childRoot + "/messages/");
  }

  public record TransportPaths(String endpoint, String sse, String messagesPrefix) {
    public boolean matches(String normalizedPath) {
      return endpoint.equals(normalizedPath)
          || (endpoint + "/").equals(normalizedPath)
          || sse.equals(normalizedPath)
          || (sse + "/").equals(normalizedPath)
          || normalizedPath.startsWith(messagesPrefix);
    }

    public String permissionPaths() {
      return String.join(
          ",",
          List.of(
                  endpoint,
                  endpoint.equals("/") ? endpoint : endpoint + "/",
                  sse,
                  sse + "/",
                  messagesPrefix + "*")
              .stream()
              .distinct()
              .toList());
    }
  }
}
