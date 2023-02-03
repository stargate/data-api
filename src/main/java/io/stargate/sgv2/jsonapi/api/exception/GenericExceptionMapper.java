package io.stargate.sgv2.jsonapi.api.exception;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Translates any exception that is not handled by the command engine during the API operation to a
 * CommandResult, with status `HTTP 200`.
 */
public class GenericExceptionMapper {

  @ServerExceptionMapper()
  public RestResponse<CommandResult> genericExceptionMapper(Exception e) {
    CommandResult commandResult = new ThrowableCommandResultSupplier(e).get();
    return RestResponse.ok(commandResult);
  }
}
