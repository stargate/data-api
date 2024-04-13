package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineWriterSessionStatus;
import java.util.List;

public record OfflineGetStatusResponse(
    OfflineWriterSessionStatus offlineWriterSessionStatus, List<CommandResult.Error> errors) {

  public static OfflineGetStatusResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new OfflineGetStatusResponse(null, commandResult.errors());
    }
    return new OfflineGetStatusResponse(
        (OfflineWriterSessionStatus)
            commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_STATUS),
        null);
  }
}
