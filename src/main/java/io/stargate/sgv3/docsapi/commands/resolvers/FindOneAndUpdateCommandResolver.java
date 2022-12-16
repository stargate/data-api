package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand.Options.ReturnDocumentOption;
import io.stargate.sgv3.docsapi.operations.DocumentUpdaterFunction;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.operations.ReadOperation;
import io.stargate.sgv3.docsapi.operations.UpdateOperation;
import io.stargate.sgv3.docsapi.operations.UpdateOperation.UpdateProjection;
import io.stargate.sgv3.docsapi.updater.DocumentUpdater;

public class FindOneAndUpdateCommandResolver extends FilterableResolver<FindOneAndUpdateCommand> {

  public FindOneAndUpdateCommandResolver() {
    super(true);
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndUpdateCommand command) {

    var readOp = (ReadOperation) super.resolveCommand(commandContext, command);

    DocumentUpdaterFunction updater =
        new DocumentUpdater(command.getUpdate(), command.options.upsert);

    UpdateProjection updateProjection =
        command.options.returnDocument == ReturnDocumentOption.BEFORE
            ? UpdateProjection.ORIGINAL
            : UpdateProjection.MODIFIED;

    return new UpdateOperation(commandContext, readOp, updater, updateProjection);
  }
}
