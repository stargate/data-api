package io.stargate.sgv2.jsonapi.exception.mappers;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.ServerExceptionMapper;

/**
 * Translates any exception that is not handled by the command engine during the API operation to a
 * CommandResult, with status `HTTP 200`.
 */
public class GenericExceptionMapper {

  // explicitly add types to override Quarkus mappers
  @ServerExceptionMapper({
    Exception.class,
    JsonParseException.class,
    MismatchedInputException.class
  })
  public RestResponse<CommandResult> genericExceptionMapper(Throwable e) {

    if (e instanceof APIException apiException) {
      // new error object V2
      var errorBuilder = new APIExceptionCommandErrorBuilder(true, false);

      return CommandResult.statusOnlyBuilder(false, true)
          .addCommandResultError(errorBuilder.buildLegacyCommandResultError(apiException))
          .build()
          .toRestResponse();
    }

    CommandResult commandResult = new ThrowableCommandResultSupplier(e).get();
    return commandResult.toRestResponse();
  }
}
