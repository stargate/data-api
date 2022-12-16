package io.stargate.sgv3.docsapi.api.v3;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.service.CommandService;
import io.stargate.sgv3.docsapi.config.constants.OpenApiConstants;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.jboss.resteasy.reactive.RestResponse;

@Path(CollectionResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
public class CollectionResource {
  @Inject CommandService commandService;
  public static final String BASE_PATH = "/v3/{database}/{collection}";

  @POST
  public Uni<RestResponse<?>> postCommand(
      @PathParam("database") String database,
      @PathParam("collection") String collection,
      @RequestBody String query) {
    return commandService
        .processCommand(database, collection, query)
        .onItem()
        .transform(res -> RestResponse.ok(res));
  }
}
