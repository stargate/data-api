package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.operations.CreateCollectionOperation;
import io.stargate.sgv3.docsapi.operations.Operation;

public class CreateCollectionResolver implements CommandResolver<CreateCollectionCommand> {

  @Override
  public Operation resolveCommand(CommandContext commandContext, CreateCollectionCommand command) {
    return new CreateCollectionOperation(commandContext, command.name);
  }
}
