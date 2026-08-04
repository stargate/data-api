package io.stargate.sgv2.jsonapi.api.v1;

import io.quarkus.security.UnauthorizedException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.health.DatabaseReadinessCheck;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Authenticated database readiness endpoint registered through Quarkus JAX-RS resource discovery.
 *
 * <p>{@code GET /v1/health/ready} runs the same request-scoped probe for Astra and Cassandra. A
 * successful probe returns HTTP 200; invalid credentials return HTTP 401; and a database failure or
 * timeout returns HTTP 503. The existing {@code /v1/*} security policy rejects requests without a
 * token before this resource is called.
 */
@Path(DatabaseReadinessResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
public class DatabaseReadinessResource {

  public static final String BASE_PATH = GeneralResource.BASE_PATH + "/health/ready";

  private static final ReadinessResponse UP = new ReadinessResponse("UP");
  private static final ReadinessResponse DOWN = new ReadinessResponse("DOWN");

  private final DatabaseReadinessCheck readinessCheck;
  private final RequestContext requestContext;

  @Inject
  public DatabaseReadinessResource(
      CqlSessionCacheSupplier sessionCacheSupplier, RequestContext requestContext) {
    this.readinessCheck = new DatabaseReadinessCheck(sessionCacheSupplier);
    this.requestContext = requestContext;
  }

  @GET
  @Operation(
      summary = "Check database readiness",
      description =
          "Uses the authenticated request tenant and token to perform a LOCAL_QUORUM read.")
  @APIResponses({
    @APIResponse(
        responseCode = "200",
        description = "The database completed the readiness read.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = ReadinessResponse.class))),
    @APIResponse(responseCode = "401", description = "The token is missing or invalid."),
    @APIResponse(
        responseCode = "503",
        description = "The database read failed or timed out.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = ReadinessResponse.class)))
  })
  public Uni<RestResponse<ReadinessResponse>> ready() {
    return readinessCheck
        .check(requestContext)
        .map(ignored -> RestResponse.ok(UP))
        .onFailure(DatabaseReadinessResource::isUnauthorized)
        .recoverWithItem(
            failure ->
                RestResponse.ResponseBuilder.create(Response.Status.UNAUTHORIZED, DOWN).build())
        .onFailure()
        .recoverWithItem(
            failure ->
                RestResponse.ResponseBuilder.create(Response.Status.SERVICE_UNAVAILABLE, DOWN)
                    .build());
  }

  private static boolean isUnauthorized(Throwable failure) {
    var current = failure;
    while (current != null) {
      if (current instanceof UnauthorizedException
          || current instanceof APISecurityException apiException
              && apiException.httpStatus == Response.Status.UNAUTHORIZED.getStatusCode()) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  public record ReadinessResponse(String status) {}
}
