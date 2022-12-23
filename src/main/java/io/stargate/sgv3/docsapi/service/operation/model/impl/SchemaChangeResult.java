package io.stargate.sgv3.docsapi.service.operation.model.impl;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.api.model.command.CommandStatus;
import java.util.Map;
import java.util.function.Supplier;

public record SchemaChangeResult(boolean schemaChanged) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.CREATE_COLLECTION, schemaChanged ? 1 : 0));
  }
}
