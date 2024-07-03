package io.stargate.sgv2.jsonapi.exception.mappers;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Translates any exception that is not handled by the command engine during the API operation to a
 * CommandResult, with status `HTTP 200`.
 */
public class GenericExceptionMapper {

  // explicitly add types to override Quarkus mappers
  @ServerExceptionMapper({Exception.class})
  public RestResponse<CommandResult> genericExceptionMapper(Throwable e) {
    CommandResult commandResult = new ThrowableCommandResultSupplier(e).get();
    return RestResponse.ok(commandResult);
  }
}
