package io.stargate.sgv2.jsonapi.service.operation.collections;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import java.util.function.Supplier;

public record ServiceRegistrationResult() implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return CommandResult.singleDocumentBuilder(false, false, null)
        .addStatus(CommandStatus.OK, 1)
        .build();
  }
}
