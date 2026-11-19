package io.stargate.sgv2.jsonapi.api.security;

import io.stargate.sgv2.jsonapi.api.v1.mcp.McpOriginFilter;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Publishes the authorization server for the MCP resource the request addressed. */
@Path(ProtectedResourceMetadataResource.PATH)
public class ProtectedResourceMetadataResource {
  public static final String PATH = "/.well-known/oauth-protected-resource";

  private final McpOAuthMetadata metadata;
  private final Provider<RoutingContext> routingContext;
  private final McpOriginFilter origins;

  @Inject
  public ProtectedResourceMetadataResource(
      McpOAuthMetadata metadata, Provider<RoutingContext> routingContext, McpOriginFilter origins) {
    this.metadata = metadata;
    this.routingContext = routingContext;
    this.origins = origins;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public McpOAuthMetadata.ProtectedResourceMetadata get() {
    var context = routingContext.get();
    var document = metadata.document(context).orElseThrow(NotFoundException::new);
    var requestOrigins = context.request().headers().getAll(HttpHeaders.ORIGIN);
    context.response().headers().add(HttpHeaders.VARY, "Origin");
    if (origins.allowsOrigin(requestOrigins)) {
      context
          .response()
          .putHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, requestOrigins.getFirst());
    }
    return document;
  }
}
