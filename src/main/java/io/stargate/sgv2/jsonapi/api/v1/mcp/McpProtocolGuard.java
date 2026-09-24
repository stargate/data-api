package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.security.HeaderBasedAuthenticationMechanism;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/** Enforces the single MCP revision before requests reach Quarkiverse's multi-version transport. */
@ApplicationScoped
public class McpProtocolGuard {

  static final String VERSION = "2026-07-28";
  private static final String ROOT = "/v1/mcp";
  private static final String VERSION_META = "io.modelcontextprotocol/protocolVersion";
  private static final String CAPABILITIES_META = "io.modelcontextprotocol/clientCapabilities";
  private static final String CLIENT_INFO_META = "io.modelcontextprotocol/clientInfo";
  private static final String SERVER_INFO_META = "io.modelcontextprotocol/serverInfo";

  @Inject AuthConfig authConfig;
  @Inject MeterRegistry meterRegistry;

  @ConfigProperty(name = "quarkus.application.name", defaultValue = "sgv2-jsonapi")
  String applicationName;

  @ConfigProperty(name = "quarkus.application.version", defaultValue = "unknown")
  String applicationVersion;

  @ConfigProperty(name = "stargate.mcp.allowed-origins")
  Optional<List<String>> allowedOrigins;

  void register(@Observes Router router) {
    router.route(ROOT).order(-2).handler(this::guardHeaders);
    router.route(ROOT).order(-1).handler(BodyHandler.create()).handler(this::guardBody);
    router.route(ROOT + "/sse").order(-1).handler(this::closeLegacyRoute);
    router.route(ROOT + "/messages/:id").order(-1).handler(this::closeLegacyRoute);
  }

  private void guardHeaders(RoutingContext context) {
    if (!validOrigin(context)) {
      return;
    }
    if (!authenticated(context)) {
      return;
    }
    HttpMethod method = context.request().method();
    if (method == HttpMethod.GET || method == HttpMethod.DELETE) {
      context.response().putHeader("Allow", "POST").setStatusCode(405).end();
      return;
    }
    if (method != HttpMethod.POST) {
      context.next();
      return;
    }

    String version = context.request().getHeader("MCP-Protocol-Version");
    if (version == null || version.isBlank()) {
      meterRegistry.counter("mcp.protocol.rejections", "reason", "missing_version").increment();
      sendError(context, null, 400, -32020, "Missing MCP-Protocol-Version header", null);
      return;
    }
    if (!VERSION.equals(version)) {
      meterRegistry.counter("mcp.protocol.rejections", "reason", "unsupported_version").increment();
      sendError(
          context,
          null,
          400,
          -32022,
          "Unsupported protocol version",
          new JsonObject()
              .put("requested", version)
              .put("supported", new JsonArray().add(VERSION)));
      return;
    }

    String mcpMethod = context.request().getHeader("Mcp-Method");
    if (mcpMethod == null || mcpMethod.isBlank()) {
      sendError(context, null, 400, -32020, "Missing Mcp-Method header", null);
      return;
    }
    if (!"server/discover".equals(mcpMethod)
        && !"tools/list".equals(mcpMethod)
        && !"tools/call".equals(mcpMethod)) {
      sendError(context, null, 404, -32601, "Method not found", null);
      return;
    }
    context.next();
  }

  private void guardBody(RoutingContext context) {
    if ("server/discover".equals(context.request().getHeader("Mcp-Method"))) {
      try {
        discover(context, context.getBodyAsJson());
      } catch (DecodeException e) {
        sendError(context, null, 400, -32700, "Invalid JSON request", null);
      }
    } else {
      context.next();
    }
  }

  private void closeLegacyRoute(RoutingContext context) {
    if (validOrigin(context) && authenticated(context)) {
      context.response().setStatusCode(404).end();
    }
  }

  private boolean validOrigin(RoutingContext context) {
    String origin = context.request().getHeader("Origin");
    if (origin != null && !allowedOrigins.orElseGet(List::of).contains(origin)) {
      meterRegistry.counter("mcp.origin.rejections").increment();
      context.response().setStatusCode(403).end();
      return false;
    }
    return true;
  }

  /** The header-based mechanism's authentication check is mirrored before any guard response. */
  private boolean authenticated(RoutingContext context) {
    if (authConfig.headerBased().enabled()
        && HeaderBasedAuthenticationMechanism.authenticationHeader(
                context, authConfig.headerBased().headerName())
            == null) {
      meterRegistry.counter("mcp.authentication.failures").increment();
      context.response().setStatusCode(401).end();
      return false;
    }
    return true;
  }

  private void discover(RoutingContext context, JsonObject request) {
    if (request == null) {
      sendError(context, null, 400, -32700, "Invalid JSON request", null);
      return;
    }
    Object id = request.getValue("id");
    if (!"2.0".equals(request.getValue("jsonrpc"))
        || !(id instanceof String || id instanceof Integer || id instanceof Long)) {
      sendError(context, null, 400, -32600, "Invalid JSON-RPC request", null);
      return;
    }
    if (!"server/discover".equals(request.getValue("method"))) {
      sendError(context, id, 400, -32020, "Mcp-Method does not match request method", null);
      return;
    }
    JsonObject params = request.getValue("params") instanceof JsonObject value ? value : null;
    JsonObject meta =
        params != null && params.getValue("_meta") instanceof JsonObject value ? value : null;
    if (meta == null
        || !(meta.getValue(VERSION_META) instanceof String)
        || !(meta.getValue(CAPABILITIES_META) instanceof JsonObject)
        || (meta.containsKey(CLIENT_INFO_META)
            && !(meta.getValue(CLIENT_INFO_META) instanceof JsonObject))) {
      sendError(context, id, 400, -32602, "Invalid MCP request metadata", null);
      return;
    }
    if (!VERSION.equals(meta.getValue(VERSION_META))) {
      sendError(
          context, id, 400, -32020, "MCP-Protocol-Version does not match request metadata", null);
      return;
    }

    JsonObject serverInfo =
        new JsonObject().put("name", applicationName).put("version", applicationVersion);
    JsonObject result =
        new JsonObject()
            .put("resultType", "complete")
            .put("supportedVersions", new JsonArray().add(VERSION))
            .put("capabilities", new JsonObject().put("tools", new JsonObject()))
            .put("ttlMs", 0)
            .put("cacheScope", "private")
            .put("_meta", new JsonObject().put(SERVER_INFO_META, serverInfo))
            // Quarkiverse 2.0.1's stateless test client still reads this pre-final location.
            .put("serverInfo", serverInfo);
    context
        .response()
        .putHeader("Content-Type", "application/json")
        .end(new JsonObject().put("jsonrpc", "2.0").put("id", id).put("result", result).encode());
  }

  private static void sendError(
      RoutingContext context, Object id, int status, int code, String message, JsonObject data) {
    JsonObject error = new JsonObject().put("code", code).put("message", message);
    if (data != null) {
      error.put("data", data);
    }
    JsonObject response = new JsonObject().put("jsonrpc", "2.0").put("error", error);
    if (id != null) {
      response.put("id", id);
    }
    context
        .response()
        .setStatusCode(status)
        .putHeader("Content-Type", "application/json")
        .end(response.encode());
  }
}
