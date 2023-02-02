package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;

import java.util.Map;
import java.util.function.Supplier;

public record SchemaChangeResult(boolean schemaChanged) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.OK, schemaChanged ? 1 : 0));
  }
}
