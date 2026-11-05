package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.vertx.http.runtime.filters.Filters;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Records MCP outcomes using server-generated identifiers and fixed categories only. */
@ApplicationScoped
public class McpRequestLogging {
  static final String REQUEST_ID = "mcpRequestId";
  private static final Logger LOGGER = LoggerFactory.getLogger(McpRequestLogging.class);
  private static final Set<String> METHODS = Set.of("server/discover", "tools/list", "tools/call");

  @ConfigProperty(name = McpEndpointConfigSourceFactory.ENDPOINT_PATH)
  String endpointPath;

  void register(@Observes Filters filters) {
    // Run before authentication and MCP parsing, so failures there are protected too.
    filters.register(this::handle, Integer.MAX_VALUE);
  }

  void handle(RoutingContext context) {
    String path = context.normalizedPath();
    if (!endpointPath.equals("/")
        && !path.equals(endpointPath)
        && !path.startsWith(endpointPath + "/")) {
      context.next();
      return;
    }
    String requestId = RequestContext.getOrCreateRequestId(context);
    String header = context.request().getHeader("Mcp-Method");
    String method = header != null && METHODS.contains(header) ? header : "other";
    MDC.put(REQUEST_ID, requestId);
    context.addEndHandler(
        ignored -> {
          LOGGER.info(
              "MCP request completed requestId={} method={} status={}",
              requestId,
              method,
              context.response().getStatusCode());
          MDC.remove(REQUEST_ID);
        });
    context.next();
  }

  static void activate(RoutingContext context) {
    // The framework runs tools in a new duplicated Vert.x context, without the HTTP MDC.
    // RoutingContext data survives that boundary and the ID never comes from client input.
    MDC.put(REQUEST_ID, RequestContext.getOrCreateRequestId(context));
  }

  static void commandCompleted(Command command, ToolResponse result) {
    LOGGER.info(
        "MCP command completed requestId={} command={} isError={}",
        MDC.get(REQUEST_ID),
        command.commandName().getApiName(),
        result.isError());
  }

  static void setupFailed() {
    LOGGER.warn("MCP setup failed requestId={} category=unexpected", MDC.get(REQUEST_ID));
  }

  static void commandFailed(Command command) {
    LOGGER.warn(
        "MCP command failed requestId={} command={}",
        MDC.get(REQUEST_ID),
        command.commandName().getApiName());
  }
}
