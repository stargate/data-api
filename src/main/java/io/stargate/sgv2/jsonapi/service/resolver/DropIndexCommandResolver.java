package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AddIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolver for the {@link AddIndexCommand}. */
@ApplicationScoped
public class DropIndexCommandResolver implements CommandResolver<DropIndexCommand> {
  @Override
  public Class<DropIndexCommand> getCommandClass() {
    return DropIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, DropIndexCommand command) {
    return new DropIndexOperation(ctx, command.indexName());
  }
}
