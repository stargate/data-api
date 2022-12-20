package io.stargate.sgv3.docsapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Operation that inserts one or more documents. */
public record InsertOperation(
    CommandContext commandContext, List<WritableShreddedDocument> documents) implements Operation {

  public InsertOperation(CommandContext commandContext, WritableShreddedDocument document) {
    this(commandContext, List.of(document));
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(StargateBridge bridge) {
    // TODO implement me
    Supplier<CommandResult> supplier =
        () -> new CommandResult(new CommandResult.ResponseData(Collections.emptyList()));
    return Uni.createFrom().<Supplier<CommandResult>>item(supplier);
  }
}
