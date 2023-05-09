package io.stargate.sgv2.jsonapi.api.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Tries to omit the `WebApplicationException` and just report the cause. */
public class WebApplicationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<CommandResult> genericExceptionMapper(WebApplicationException e) {
    Throwable toReport = null != e.getCause() ? e.getCause() : e;
    CommandResult commandResult = new ThrowableCommandResultSupplier(toReport).get();
    // Return 405 for method not allowed and 404 for not found
    if (e instanceof NotAllowedException) {
      return RestResponse.status(RestResponse.Status.METHOD_NOT_ALLOWED, commandResult);
    }
    if (e instanceof NotFoundException) {
      return RestResponse.status(RestResponse.Status.NOT_FOUND, commandResult);
    }
    return RestResponse.ok(commandResult);
  }
}
