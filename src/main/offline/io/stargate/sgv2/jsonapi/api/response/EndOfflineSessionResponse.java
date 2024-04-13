package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineWriterSessionStatus;
import java.util.List;

public record EndOfflineSessionResponse(
    OfflineWriterSessionStatus offlineWriterSessionStatus, List<CommandResult.Error> errors) {
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
