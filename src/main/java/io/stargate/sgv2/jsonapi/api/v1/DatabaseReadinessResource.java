package io.stargate.sgv2.jsonapi.api.v1;

import io.quarkus.security.UnauthorizedException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.health.DatabaseReadinessCheck;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
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
import java.util.Collections;
import java.util.IdentityHashMap;
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
 * request must use the configured SLA User-Agent. A successful probe returns HTTP 200; invalid
 * credentials return HTTP 401; a missing or different SLA User-Agent returns HTTP 403; and a
 * database failure, timeout, or missing SLA configuration returns HTTP 503. The existing {@code
 * /v1/*} security policy rejects requests without a token before this resource is called.
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
  private final CqlSessionCacheSupplier sessionCacheSupplier;

  @Inject
  public DatabaseReadinessResource(
      CqlSessionCacheSupplier sessionCacheSupplier, RequestContext requestContext) {
    this.readinessCheck = new DatabaseReadinessCheck(sessionCacheSupplier);
    this.requestContext = requestContext;
    this.sessionCacheSupplier = sessionCacheSupplier;
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
    @APIResponse(
        responseCode = "401",
        description = "The token is missing or invalid.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = CommandResult.class))),
    @APIResponse(
        responseCode = "403",
        description = "The request does not use the configured SLA User-Agent."),
    @APIResponse(
        responseCode = "503",
        description = "The SLA User-Agent is not configured, or the database check failed.",
        content =
            @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(implementation = ReadinessResponse.class)))
  })
  public Uni<RestResponse<Object>> ready() {
    var configuredSlaUserAgent = sessionCacheSupplier.slaUserAgent();
    if (configuredSlaUserAgent.isEmpty()) {
      return Uni.createFrom().item(response(Response.Status.SERVICE_UNAVAILABLE, DOWN));
    }
    if (!configuredSlaUserAgent.get().equals(requestContext.userAgent())) {
      return Uni.createFrom().item(response(Response.Status.FORBIDDEN));
    }

    return readinessCheck
        .check(requestContext)
        .map(ignored -> response(Response.Status.OK, UP))
        .onFailure(DatabaseReadinessResource::isUnauthorized)
        .recoverWithItem(failure -> unauthorizedResponse())
        .onFailure()
        .recoverWithItem(failure -> response(Response.Status.SERVICE_UNAVAILABLE, DOWN));
  }

  private static boolean isUnauthorized(Throwable failure) {
    var current = failure;
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    while (current != null && seen.add(current)) {
      if (current instanceof UnauthorizedException
          || current instanceof APISecurityException apiException
              && apiException.httpStatus == Response.Status.UNAUTHORIZED.getStatusCode()) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static RestResponse<Object> unauthorizedResponse() {
    var commandResult =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addThrowable(APISecurityException.Code.UNAUTHENTICATED_REQUEST.get())
            .build();
    return response(Response.Status.UNAUTHORIZED, commandResult);
  }

  private static RestResponse<Object> response(Response.Status status) {
    return RestResponse.ResponseBuilder.<Object>create(status).build();
  }

  private static RestResponse<Object> response(Response.Status status, Object entity) {
    return RestResponse.ResponseBuilder.<Object>create(status, entity).build();
  }

  public record ReadinessResponse(String status) {}
}
