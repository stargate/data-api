package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This represents the response for a delete operation.
 *
 * @param deletedCount - Count of documents deleted
 * @param moreData - if `true` means more documents available in DB for the provided condition
 */
public record DeleteOperationPage(Integer deletedCount, boolean moreData)
    implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    if (moreData)
      return new CommandResult(
          Map.of(CommandStatus.DELETED_COUNT, deletedCount, CommandStatus.MORE_DATA, true));
    else return new CommandResult(Map.of(CommandStatus.DELETED_COUNT, deletedCount));
  }
}
