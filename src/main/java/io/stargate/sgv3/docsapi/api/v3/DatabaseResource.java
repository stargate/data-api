package io.stargate.sgv3.docsapi.api.v3;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.config.constants.OpenApiConstants;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

@Path(DatabaseResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(name = "Databases", description = "Executes database commands.")
public class DatabaseResource {

  public static final String BASE_PATH = "/v3/{database}";

  @POST
  public Uni<RestResponse<?>> postCommand() {
    return Uni.createFrom().item(RestResponse.ok());
  }
}
