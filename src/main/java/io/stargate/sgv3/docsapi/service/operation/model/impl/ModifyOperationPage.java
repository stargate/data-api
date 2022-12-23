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
public class ModifyOperationPage implements Supplier<CommandResult> {

  public final List<String> insertedIds;
  public final List<WritableShreddedDocument> insertedDocs;

  private ModifyOperationPage(
      List<String> insertedIds, List<WritableShreddedDocument> insertedDocs) {
    this.insertedIds = insertedIds;
    this.insertedDocs = insertedDocs;
  }

  public static ModifyOperationPage from(
      List<String> insertedIds, List<WritableShreddedDocument> insertedDocs) {
    return new ModifyOperationPage(insertedIds, insertedDocs);
  }

  @Override
  public CommandResult get() {
    return new CommandResult(Map.of(CommandStatus.INSERTED_IDS, insertedIds));
  }
}
