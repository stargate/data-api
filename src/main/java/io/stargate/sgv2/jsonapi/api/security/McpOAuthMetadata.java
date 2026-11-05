package io.stargate.sgv2.jsonapi.api.security;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.Startup;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.stargate.sgv2.jsonapi.api.v1.mcp.McpEndpointConfigSourceFactory;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import io.stargate.sgv2.jsonapi.config.McpAuthConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Keeps discovery and authentication challenges on the same configured public resource. */
@Startup
@ApplicationScoped
public class McpOAuthMetadata {
  private final McpEndpointConfigSourceFactory.TransportPaths transportPaths;
  private final Optional<ProtectedResourceMetadata> document;
  private final Optional<String> challenge;

  @Inject
  public McpOAuthMetadata(
      AuthConfig authConfig,
      FeaturesConfig featuresConfig,
      McpAuthConfig config,
      @ConfigProperty(name = McpEndpointConfigSourceFactory.ENDPOINT_PATH) String endpointPath,
      @ConfigProperty(name = McpEndpointConfigSourceFactory.METADATA_PATH) String metadataPath,
      JWTAuthContextInfo jwtConfig) {
    this.transportPaths = McpEndpointConfigSourceFactory.transportPaths(endpointPath);
    boolean bearerEnabled = authConfig.bearer().enabled();
    String httpRoot =
        metadataPath.substring(
            0, metadataPath.length() - ProtectedResourceMetadataResource.PATH.length());
    if (bearerEnabled
        && (endpointPath.equals(httpRoot.isEmpty() ? "/" : httpRoot)
            || !endpointPath.startsWith(httpRoot + "/"))) {
      // SDK 2.0.3 registers root endpoints on a different router from their auth permissions.
      throw new IllegalArgumentException(
          "OAuth requires quarkus.mcp.server.http.root-path to use a dedicated path below the HTTP root");
    }
    if (!bearerEnabled
        || !ApiFeatures.fromConfigAndRequest(featuresConfig, null)
            .isFeatureEnabled(ApiFeature.MCP)) {
      document = Optional.empty();
      challenge = Optional.empty();
      return;
    }

    URI publicBase =
        config
            .publicBaseUrl()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("stargate.mcp.auth.public-base-url is required"));
    validateUrl(publicBase, "stargate.mcp.auth.public-base-url", true);
    URI authorizationServer =
        config
            .serverUrl()
            .orElseThrow(
                () -> new IllegalArgumentException("stargate.mcp.auth.server-url is required"));
    validateUrl(authorizationServer, "stargate.mcp.auth.server-url", false);
    URI resource = publicBase.resolve(endpointPath);
    if (jwtConfig.getExpectedAudience() == null
        || !jwtConfig.getExpectedAudience().contains(resource.toString())) {
      throw new IllegalArgumentException(
          "mp.jwt.verify.audiences must include the configured public MCP resource");
    }
    document =
        Optional.of(
            new ProtectedResourceMetadata(
                resource,
                List.of(authorizationServer),
                List.of("header"),
                List.copyOf(config.scopesSupported().orElseGet(List::of))));
    challenge =
        Optional.of(
            "Bearer resource_metadata=\"" + publicBase.resolve(metadataPath).toString() + "\"");
  }

  public Optional<ProtectedResourceMetadata> document() {
    return document;
  }

  public boolean isMcpRequest(RoutingContext context) {
    return transportPaths.matches(context.normalizedPath());
  }

  public Optional<String> challenge(RoutingContext context) {
    if (challenge.isEmpty()) {
      return Optional.empty();
    }
    return isMcpRequest(context) ? challenge : Optional.empty();
  }

  private static void validateUrl(URI uri, String property, boolean originOnly) {
    String scheme = uri.getScheme();
    String host = uri.getHost();
    boolean loopback =
        "localhost".equalsIgnoreCase(host)
            || "127.0.0.1".equals(host)
            || "::1".equals(host)
            || "[::1]".equals(host);
    boolean secure =
        "https".equalsIgnoreCase(scheme) || ("http".equalsIgnoreCase(scheme) && loopback);
    if (!secure
        || host == null
        || uri.getUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null
        || (originOnly
            && uri.getRawPath() != null
            && !uri.getRawPath().isEmpty()
            && !uri.getRawPath().equals("/"))) {
      throw new IllegalArgumentException(
          property
              + " must be an HTTPS URL or loopback HTTP URL"
              + (originOnly ? " without a path" : ""));
    }
  }

  public record ProtectedResourceMetadata(
      URI resource,
      @JsonProperty("authorization_servers") List<URI> authorizationServers,
      @JsonProperty("bearer_methods_supported") List<String> bearerMethodsSupported,
      @JsonProperty("scopes_supported") @JsonInclude(JsonInclude.Include.NON_EMPTY)
          List<String> scopesSupported) {}
}
