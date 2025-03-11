package io.stargate.sgv2.jsonapi.service.operation.collections;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.function.Supplier;

public record SchemaChangeResult(boolean schemaChanged) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return CommandResult.statusOnlyBuilder(false, false, null)
        .addStatus(CommandStatus.OK, schemaChanged ? 1 : 0)
        .build();
  }
}
