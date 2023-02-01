package io.stargate.sgv3.docsapi.service.operation.model.impl;

import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.api.model.command.CommandStatus;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The internal to modification operation results, what were the ID's of the docs we changed and
 * what change.
 */
public record InsertOperationPage(
    List<Object> insertedIds, List<WritableShreddedDocument> insertedDocs)
    implements Supplier<CommandResult> {
  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.INSERTED_IDS, insertedIds));
  }
}
