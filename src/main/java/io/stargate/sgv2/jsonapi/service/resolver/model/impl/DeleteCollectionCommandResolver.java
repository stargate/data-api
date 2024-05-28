package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolver for the {@link DeleteCollectionCommand}. */
@ApplicationScoped
public class DeleteCollectionCommandResolver implements CommandResolver<DeleteCollectionCommand> {
  @Override
  public Class<DeleteCollectionCommand> getCommandClass() {
    return DeleteCollectionCommand.class;
  }

  @Inject private SchemaCache schemaCache;

  @Override
  public Operation resolveCommand(CommandContext ctx, DeleteCollectionCommand command) {
    return new DeleteCollectionOperation(ctx, command.name());
  }
}
