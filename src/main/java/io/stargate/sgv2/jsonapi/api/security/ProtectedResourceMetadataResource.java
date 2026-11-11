package io.stargate.sgv2.jsonapi.api.security;

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

  @Inject
  public ProtectedResourceMetadataResource(
      McpOAuthMetadata metadata, Provider<RoutingContext> routingContext) {
    this.metadata = metadata;
    this.routingContext = routingContext;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public McpOAuthMetadata.ProtectedResourceMetadata get() {
    return metadata.document(routingContext.get()).orElseThrow(NotFoundException::new);
  }
}
