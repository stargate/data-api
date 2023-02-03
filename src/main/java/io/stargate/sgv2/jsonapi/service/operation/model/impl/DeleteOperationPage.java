package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This represents the response for a delete operation. .
 *
 * @param deletedIds - document ids deleted
 */
public record DeleteOperationPage(List<DocumentId> deletedIds) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.DELETED_IDS, deletedIds));
  }
}
