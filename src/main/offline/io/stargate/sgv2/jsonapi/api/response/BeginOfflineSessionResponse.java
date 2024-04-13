package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.List;

public record BeginOfflineSessionResponse(String sessionId, List<CommandResult.Error> errors) {

  public static BeginOfflineSessionResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new BeginOfflineSessionResponse(null, commandResult.errors());
    }
    return new BeginOfflineSessionResponse(
        commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_ID).toString(), null);
  }
}
