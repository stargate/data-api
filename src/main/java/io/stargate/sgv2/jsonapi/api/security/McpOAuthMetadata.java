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
import io.vertx.core.net.HostAndPort;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Keeps discovery, authentication challenges and audience checks on the same public resource.
 *
 * <p>The resource is the public URL of the MCP endpoint. A deployment with one public host
 * configures it with {@code stargate.mcp.auth.public-base-url}. A deployment that serves one public
 * host per database configures {@code stargate.mcp.auth.public-host-suffix} instead, and each
 * request host under that suffix is its own resource.
 */
@Startup
@ApplicationScoped
public class McpOAuthMetadata {
  private static final String PUBLIC_BASE_URL = "stargate.mcp.auth.public-base-url";
  private static final String PUBLIC_HOST_SUFFIX = "stargate.mcp.auth.public-host-suffix";
  private static final int HTTPS_PORT = 443;

  private static final String DNS_LABEL = "[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?";
  private static final Pattern HOST_LABEL = Pattern.compile(DNS_LABEL);
  private static final Pattern HOST_SUFFIX = Pattern.compile("(?:\\." + DNS_LABEL + "){2,}");

  private final McpEndpointConfigSourceFactory.TransportPaths transportPaths;
  private final String endpointPath;
  private final String metadataPath;

  private final boolean bearerEnabled;

  /** True when Bearer authentication and MCP are both enabled in server configuration. */
  private final boolean published;

  /** Null unless each request host is its own resource. */
  private final String hostSuffix;

  /** Null unless the deployment publishes one fixed resource. */
  private final URI fixedBase;

  private final List<URI> authorizationServers;
  private final List<String> scopesSupported;

  @Inject
  public McpOAuthMetadata(
      AuthConfig authConfig,
      FeaturesConfig featuresConfig,
      McpAuthConfig config,
      @ConfigProperty(name = McpEndpointConfigSourceFactory.ENDPOINT_PATH) String endpointPath,
      @ConfigProperty(name = McpEndpointConfigSourceFactory.METADATA_PATH) String metadataPath,
      JWTAuthContextInfo jwtConfig) {
    this.transportPaths = McpEndpointConfigSourceFactory.transportPaths(endpointPath);
    this.endpointPath = endpointPath;
    this.metadataPath = metadataPath;
    this.bearerEnabled = authConfig.bearer().enabled();
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
    // The audience check must hold whenever a Bearer token can reach MCP, and a request header can
    // enable MCP when server configuration leaves the feature undefined.
    this.hostSuffix = bearerEnabled ? hostSuffix(config) : null;
    this.published =
        bearerEnabled
            && ApiFeatures.fromConfigAndRequest(featuresConfig, null)
                .isFeatureEnabled(ApiFeature.MCP);
    this.fixedBase =
        bearerEnabled && hostSuffix == null ? config.publicBaseUrl().orElse(null) : null;
    if (fixedBase != null) {
      validateUrl(fixedBase, PUBLIC_BASE_URL, true);
    }
    if (!published) {
      authorizationServers = List.of();
      scopesSupported = List.of();
      return;
    }

    if (hostSuffix == null && fixedBase == null) {
      throw new IllegalArgumentException(
          PUBLIC_BASE_URL + " or " + PUBLIC_HOST_SUFFIX + " is required");
    }
    URI authorizationServer =
        config
            .serverUrl()
            .orElseThrow(
                () -> new IllegalArgumentException("stargate.mcp.auth.server-url is required"));
    validateUrl(authorizationServer, "stargate.mcp.auth.server-url", false);
    if (fixedBase != null
        && (jwtConfig.getExpectedAudience() == null
            || !jwtConfig
                .getExpectedAudience()
                .contains(fixedBase.resolve(endpointPath).toString()))) {
      throw new IllegalArgumentException(
          "mp.jwt.verify.audiences must include the configured public MCP resource");
    }
    this.authorizationServers = List.of(authorizationServer);
    this.scopesSupported = List.copyOf(config.scopesSupported().orElseGet(List::of));
  }

  /** Empty when discovery is disabled or the request host is not one this deployment serves. */
  public Optional<ProtectedResourceMetadata> document(RoutingContext context) {
    return publicBase(context)
        .map(
            base ->
                new ProtectedResourceMetadata(
                    base.resolve(endpointPath),
                    authorizationServers,
                    List.of("header"),
                    scopesSupported));
  }

  public boolean isMcpRequest(RoutingContext context) {
    return transportPaths.matches(context.normalizedPath());
  }

  public Optional<String> challenge(RoutingContext context) {
    if (!published || !isMcpRequest(context)) {
      return Optional.empty();
    }
    return publicBase(context)
        .map(base -> "Bearer resource_metadata=\"" + base.resolve(metadataPath) + "\"");
  }

  public String insufficientScopeChallenge(RoutingContext context, String scope) {
    return challenge(context).map(value -> value + ", ").orElse("Bearer ")
        + "error=\"insufficient_scope\", scope=\""
        + scope
        + "\"";
  }

  public boolean bearerEnabled() {
    return bearerEnabled;
  }

  /** Requires a single audience matching the canonical MCP resource. */
  public boolean acceptsAudience(RoutingContext context, Set<String> audience) {
    return audience != null
        && audience.size() == 1
        && resourceBase(context)
            .map(base -> base.resolve(endpointPath).toString())
            .filter(audience::contains)
            .isPresent();
  }

  /** True when each request host is its own audience, so no static audience list applies. */
  public boolean audienceFromRequestHost() {
    return hostSuffix != null;
  }

  private Optional<URI> publicBase(RoutingContext context) {
    return published ? resourceBase(context) : Optional.empty();
  }

  private Optional<URI> resourceBase(RoutingContext context) {
    return hostSuffix == null ? Optional.ofNullable(fixedBase) : requestBase(context);
  }

  /**
   * The public origin of the request host, when it is a single label under the configured suffix.
   * Any other host is refused rather than echoed, so a token issued for another service cannot be
   * replayed here under that service's host name.
   */
  private Optional<URI> requestBase(RoutingContext context) {
    HostAndPort authority = context.request().authority();
    if (authority == null || authority.host() == null) {
      return Optional.empty();
    }
    String host = authority.host().toLowerCase(Locale.ROOT);
    if (!host.endsWith(hostSuffix)
        || !HOST_LABEL.matcher(host.substring(0, host.length() - hostSuffix.length())).matches()) {
      return Optional.empty();
    }
    int port = authority.port();
    return Optional.of(
        URI.create("https://" + host + (port > 0 && port != HTTPS_PORT ? ":" + port : "")));
  }

  private static String hostSuffix(McpAuthConfig config) {
    Optional<String> suffix = config.publicHostSuffix();
    if (suffix.isEmpty()) {
      return null;
    }
    if (config.publicBaseUrl().isPresent()) {
      throw new IllegalArgumentException(
          "Set only one of " + PUBLIC_BASE_URL + " and " + PUBLIC_HOST_SUFFIX);
    }
    if (!HOST_SUFFIX.matcher(suffix.get()).matches()) {
      throw new IllegalArgumentException(
          PUBLIC_HOST_SUFFIX
              + " must be a lowercase DNS suffix that starts with a dot and has at least two labels");
    }
    return suffix.get();
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
