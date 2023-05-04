package io.stargate.sgv2.jsonapi.api.v1.response;

import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import java.util.Optional;
import org.jboss.resteasy.reactive.RestResponse;

public class MapToRestResult {
  public static RestResponse map(CommandResult commandResult) {
    if (null != commandResult.errors() && !commandResult.errors().isEmpty()) {
      final Optional<CommandResult.Error> first =
          commandResult.errors().stream().filter(error -> error.errorCode() != 200).findFirst();
      if (first.isPresent()) {
        final RestResponse.Status status =
            RestResponse.Status.fromStatusCode(first.get().errorCode());
        return RestResponse.ResponseBuilder.create(
                status, new ApiError(first.get().message(), first.get().errorCode()))
            .build();
      }
    }
    return RestResponse.ok(commandResult);
  }
}
