package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolver for the {@link DeleteCollectionCommand}. */
@ApplicationScoped
public class DeleteCollectionCommandResolver implements CommandResolver<DeleteCollectionCommand> {
  @Override
  public Class<DeleteCollectionCommand> getCommandClass() {
    return DeleteCollectionCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, DeleteCollectionCommand command) {
    return new DeleteCollectionCollectionOperation(ctx, command.name());
  }
}
