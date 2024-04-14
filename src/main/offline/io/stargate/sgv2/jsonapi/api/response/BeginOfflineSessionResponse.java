package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import java.util.List;

public record BeginOfflineSessionResponse(
    /* The session ID  */
    String sessionId,
    /* List of errors */
    List<CommandResult.Error> errors) {

  /**
   * Create a new instance of {@link BeginOfflineSessionResponse} from a {@link CommandResult}.
   *
   * @param commandResult The command result returned by the {@link CommandProcessor}
   * @return A new instance of {@link BeginOfflineSessionResponse} created from the {@link
   *     CommandResult}
   */
  public static BeginOfflineSessionResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new BeginOfflineSessionResponse(null, commandResult.errors());
    }
    return new BeginOfflineSessionResponse(
        commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_ID).toString(), null);
  }
}
