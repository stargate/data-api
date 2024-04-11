package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;

public record OfflineInsertManyResponse(
    List<DocumentId> insertedIds, List<CommandResult.Error> errors) {

  public static OfflineInsertManyResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new OfflineInsertManyResponse(null, commandResult.errors());
    }
    return new OfflineInsertManyResponse(
        (List<DocumentId>) commandResult.status().get(CommandStatus.INSERTED_IDS), null);
  }
}
