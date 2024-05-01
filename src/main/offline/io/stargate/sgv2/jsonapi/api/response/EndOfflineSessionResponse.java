package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineWriterSessionStatus;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import java.util.List;

public record EndOfflineSessionResponse(
    /* The session status */
    OfflineWriterSessionStatus offlineWriterSessionStatus,
    /* List of errors */
    List<CommandResult.Error> errors) {
  /**
   * Create a new instance of {@link EndOfflineSessionResponse} from a {@link CommandResult}.
   *
   * @param commandResult The command result returned by the {@link CommandProcessor}
   * @return A new instance of {@link EndOfflineSessionResponse} created from the {@link
   *     CommandResult}
   */
  public static EndOfflineSessionResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new EndOfflineSessionResponse(null, commandResult.errors());
    }
    return new EndOfflineSessionResponse(
        (OfflineWriterSessionStatus)
            commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_STATUS),
        null);
  }
}
