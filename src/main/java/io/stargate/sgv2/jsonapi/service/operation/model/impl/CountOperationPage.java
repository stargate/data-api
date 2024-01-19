package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.Map;
import java.util.function.Supplier;

public record CountOperationPage(long count, boolean moreData) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.COUNTED_DOCUMENT, count()));
  }
}
