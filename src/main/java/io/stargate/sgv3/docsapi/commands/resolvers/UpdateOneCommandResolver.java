package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.UpdateOneCommand;
import io.stargate.sgv3.docsapi.operations.DocumentUpdaterFunction;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.operations.ReadOperation;
import io.stargate.sgv3.docsapi.operations.UpdateOperation;
import io.stargate.sgv3.docsapi.operations.UpdateOperation.UpdateProjection;
import io.stargate.sgv3.docsapi.updater.DocumentUpdater;

public class UpdateOneCommandResolver<T extends Command>
    extends FilterableResolver<UpdateOneCommand> {

  public UpdateOneCommandResolver() {
    super(true);
  }

  public Operation resolveCommand(CommandContext commandContext, UpdateOneCommand command) {

    var readOp = (ReadOperation) super.resolveCommand(commandContext, command);

    DocumentUpdaterFunction updater =
        new DocumentUpdater(command.getUpdate(), command.options.upsert);

    return new UpdateOperation(commandContext, readOp, updater, UpdateProjection.NONE);
  }
}
