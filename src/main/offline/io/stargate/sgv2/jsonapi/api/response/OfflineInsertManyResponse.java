package io.stargate.sgv2.jsonapi.api.response;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;

public record OfflineInsertManyResponse(
    /* The list of inserted document IDs */
    List<DocumentId> insertedIds,
    /* List of errors */
    List<CommandResult.Error> errors) {

  /**
   * Create a new instance of {@link OfflineInsertManyResponse} from a {@link CommandResult}.
   *
   * @param commandResult The command result returned by the {@link CommandProcessor}
   * @return A new instance of {@link OfflineInsertManyResponse} created from the {@link
   *     CommandResult}
   */
  public static OfflineInsertManyResponse fromCommandResult(CommandResult commandResult) {
    if (commandResult.errors() != null && !commandResult.errors().isEmpty()) {
      return new OfflineInsertManyResponse(null, commandResult.errors());
    }
    return new OfflineInsertManyResponse(
        (List<DocumentId>) commandResult.status().get(CommandStatus.INSERTED_IDS), null);
  }
}
