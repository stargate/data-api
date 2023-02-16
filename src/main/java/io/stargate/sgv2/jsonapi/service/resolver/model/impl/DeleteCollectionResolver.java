package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import javax.enterprise.context.ApplicationScoped;

/**
 * Resolver for the {@link DeleteCollectionCommand}.
 */
@ApplicationScoped
public class DeleteCollectionResolver implements CommandResolver<DeleteCollectionCommand> {
  @Override
  public Class<DeleteCollectionCommand> getCommandClass() {
    return DeleteCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, DeleteCollectionCommand command) {
    return new DeleteCollectionOperation(ctx, command.name());
  }
}
