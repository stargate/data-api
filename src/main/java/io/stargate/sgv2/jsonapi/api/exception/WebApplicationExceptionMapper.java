package io.stargate.sgv2.jsonapi.api.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import javax.ws.rs.WebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/** Tries to omit the `WebApplicationException` and just report the cause. */
public class WebApplicationExceptionMapper {

  @ServerExceptionMapper
  public RestResponse<CommandResult> genericExceptionMapper(WebApplicationException e) {
    Throwable toReport = null != e.getCause() ? e.getCause() : e;
    CommandResult commandResult = new ThrowableCommandResultSupplier(toReport).get();
    return RestResponse.ok(commandResult);
  }
}
