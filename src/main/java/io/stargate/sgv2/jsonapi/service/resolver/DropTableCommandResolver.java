package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionCollectionOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolver for the {@link DropTableCommand}. */
@ApplicationScoped
public class DropTableCommandResolver implements CommandResolver<DropTableCommand> {
  @Override
  public Class<DropTableCommand> getCommandClass() {
    return DropTableCommand.class;
  }

  @Override
  public Operation resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> ctx, DropTableCommand command) {
    return new DeleteCollectionCollectionOperation(ctx, command.name());
  }
}
