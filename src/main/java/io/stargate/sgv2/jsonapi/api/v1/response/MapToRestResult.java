package io.stargate.sgv2.jsonapi.api.v1.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import java.util.Optional;
import org.jboss.resteasy.reactive.RestResponse;

public class MapToRestResult {
  /**
   * Maps CommandResult to RestResponse. Except for few selective errors, all errors are mapped to http status 200.
   * In case of 401, 500, 502 and 504 response is sent with appropriate status code.
   * @param commandResult
   * @return
   */
  public static RestResponse map(CommandResult commandResult) {
    if (null != commandResult.errors() && !commandResult.errors().isEmpty()) {
      final Optional<CommandResult.Error> first =
          commandResult.errors().stream().filter(error -> error.errorCode() != 200).findFirst();
      if (first.isPresent()) {
        final RestResponse.Status status =
            RestResponse.Status.fromStatusCode(first.get().errorCode());
        return RestResponse.ResponseBuilder.create(status, commandResult).build();
      }
    }
    return RestResponse.ok(commandResult);
  }
}
