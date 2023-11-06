package io.stargate.sgv2.jsonapi.api.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Tries to omit the `WebApplicationException` and just report the cause. */
public class WebApplicationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<CommandResult> webApplicationExceptionMapper(WebApplicationException e) {
    // 06-Nov-2023, tatu: Let's dig the innermost root cause; needed f.ex for [jsonapi#448]
    //    to get to StreamConstraintsException
    Throwable toReport = e;
    while (toReport.getCause() != null) {
      toReport = toReport.getCause();
    }
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
