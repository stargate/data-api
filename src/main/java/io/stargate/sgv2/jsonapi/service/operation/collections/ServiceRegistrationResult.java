package io.stargate.sgv2.jsonapi.service.operation.collections;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.util.function.Supplier;

public record ServiceRegistrationResult() implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return CommandResult.singleDocumentBuilder(false, RequestTracing.NO_OP)
        .addStatus(CommandStatus.OK, 1)
        .build();
  }
}
