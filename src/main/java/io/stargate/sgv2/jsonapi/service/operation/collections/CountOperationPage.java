package io.stargate.sgv2.jsonapi.service.operation.collections;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import java.util.function.Supplier;

public record CountOperationPage(long count, boolean moreData) implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {

    var builder =
        CommandResult.statusOnlyBuilder(false, false, RequestTracing.NO_OP)
            .addStatus(CommandStatus.COUNTED_DOCUMENT, count);
    if (moreData) {
      builder.addStatus(CommandStatus.MORE_DATA, true);
    }
    return builder.build();
  }
}
