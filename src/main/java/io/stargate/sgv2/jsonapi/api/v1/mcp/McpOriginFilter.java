package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkus.vertx.http.runtime.filters.Filters;
import io.quarkus.vertx.http.runtime.security.SecurityHandlerPriorities;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Rejects MCP requests from browser Origins that are not explicitly approved, as the MCP Streamable
 * HTTP transport requires. Requests without an Origin header come from native clients and pass
 * through unchanged.
 *
 * <p>The check is a Quarkus HTTP filter that runs before CORS, authentication and authorization. A
 * rejected request never reaches authentication or a tool, and an approved browser can complete its
 * CORS preflight without credentials. OAuth still decides what each caller may do.
 */
@ApplicationScoped
public class McpOriginFilter {
  static final String ALLOWED_ORIGINS = "stargate.jsonapi.mcp.allowed-origins";

  // Filters run in descending priority, so this runs before CORS (300), authentication (200) and
  // authorization (100), and after host validation (400).
  static final int PRIORITY = SecurityHandlerPriorities.CORS + 50;

  static final String ALLOWED_METHODS = "POST";
  static final String ALLOWED_HEADERS =
      "Authorization, Content-Type, MCP-Protocol-Version, Mcp-Method, Mcp-Name";
  // Lets an approved browser read the OAuth challenge on a 401 or 403 response.
  static final String EXPOSED_HEADERS = "WWW-Authenticate";

  // The request is rejected before its id is read, so JSON-RPC requires a null id.
  private static final String REJECTION_BODY =
      "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32600,\"message\":\"Origin not allowed\"}}";

  private final String endpointPath;
  private final Set<String> allowedOrigins;

  @Inject
  public McpOriginFilter(
      @ConfigProperty(name = "quarkus.http.root-path", defaultValue = "/") String httpRootPath,
      @ConfigProperty(name = "quarkus.mcp.server.http.root-path", defaultValue = "/mcp")
          String mcpRootPath,
      @ConfigProperty(name = ALLOWED_ORIGINS) Optional<List<String>> allowedOrigins) {
    this.endpointPath = endpointPath(httpRootPath, mcpRootPath);
    this.allowedOrigins = approvedOrigins(allowedOrigins.orElse(List.of()));
  }

  void register(@Observes Filters filters) {
    filters.register(this::filter, PRIORITY);
  }

  void filter(RoutingContext context) {
    if (!isMcpPath(context.normalizedPath())) {
      context.next();
      return;
    }
    HttpServerRequest request = context.request();
    HttpServerResponse response = context.response();
    // Set at write time so error handlers that rebuild the response cannot drop the headers.
    context.addHeadersEndHandler(v -> response.headers().add(HttpHeaders.VARY, "Origin"));

    List<String> origins = request.headers().getAll(HttpHeaders.ORIGIN);
    if (origins.isEmpty()) {
      context.next();
      return;
    }
    String origin = origins.get(0);
    if (origins.size() > 1 || !allowedOrigins.contains(origin)) {
      response
          .setStatusCode(403)
          .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
          .end(REJECTION_BODY);
      return;
    }

    if (isPreflight(request)) {
      response
          .setStatusCode(204)
          .putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, origin)
          .putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS)
          .putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS)
          .end();
      return;
    }
    context.addHeadersEndHandler(
        v ->
            response
                .putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, origin)
                .putHeader(HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS, EXPOSED_HEADERS));
    context.next();
  }

  boolean isMcpPath(String path) {
    return path != null
        && (endpointPath.equals("/")
            || path.equals(endpointPath)
            || path.startsWith(endpointPath + "/"));
  }

  private static boolean isPreflight(HttpServerRequest request) {
    return HttpMethod.OPTIONS.equals(request.method())
        && request.headers().contains(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD);
  }

  /**
   * Resolves the MCP root path against the HTTP root path the same way Quarkus resolves the routes
   * that the MCP extension registers, so the filter covers the endpoint and every path below it.
   */
  static String endpointPath(String httpRootPath, String mcpRootPath) {
    String base = httpRootPath.endsWith("/") ? httpRootPath : httpRootPath + "/";
    String relative = mcpRootPath.startsWith("/") ? mcpRootPath.substring(1) : mcpRootPath;
    String path = URI.create(base).resolve(relative).normalize().getPath();
    return path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  /**
   * Validates configured Origins at startup. Each entry must be written exactly as a browser sends
   * it, so that request matching can be an exact string comparison with no normalization.
   */
  static Set<String> approvedOrigins(List<String> configured) {
    for (String origin : configured) {
      if (!origin.equals(serializedOrigin(origin))) {
        throw new IllegalArgumentException(
            "Invalid "
                + ALLOWED_ORIGINS
                + " entry '"
                + origin
                + "'. Use the exact browser Origin, such as https://app.example.com, with a"
                + " lowercase http or https scheme and host, no path and no default port.");
      }
    }
    return Set.copyOf(configured);
  }

  /** Returns the browser serialization of an http or https origin, or null for anything else. */
  static String serializedOrigin(String value) {
    URI uri;
    try {
      uri = new URI(value);
    } catch (URISyntaxException e) {
      return null;
    }
    String scheme = uri.getScheme();
    if (!("http".equals(scheme) || "https".equals(scheme))
        || uri.isOpaque()
        || uri.getHost() == null
        || uri.getRawUserInfo() != null
        || !uri.getRawPath().isEmpty()
        || uri.getRawQuery() != null
        || uri.getRawFragment() != null) {
      return null;
    }
    int port = uri.getPort();
    boolean defaultPort = port == -1 || port == ("https".equals(scheme) ? 443 : 80);
    return scheme
        + "://"
        + uri.getHost().toLowerCase(Locale.ROOT)
        + (defaultPort ? "" : ":" + port);
  }
}
