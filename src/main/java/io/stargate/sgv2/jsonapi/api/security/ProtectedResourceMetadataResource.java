package io.stargate.sgv2.jsonapi.api.security;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Publishes the authorization server for the configured MCP resource. */
@Path(ProtectedResourceMetadataResource.PATH)
public class ProtectedResourceMetadataResource {
  public static final String PATH = "/.well-known/oauth-protected-resource";

  private final McpOAuthMetadata metadata;

  @Inject
  public ProtectedResourceMetadataResource(McpOAuthMetadata metadata) {
    this.metadata = metadata;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public McpOAuthMetadata.ProtectedResourceMetadata get() {
    return metadata.document().orElseThrow(NotFoundException::new);
  }
}
