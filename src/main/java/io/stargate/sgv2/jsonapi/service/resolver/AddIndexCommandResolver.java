package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AddIndexCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.AddIndexOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolver for the {@link AddIndexCommand}. */
@ApplicationScoped
public class AddIndexCommandResolver implements CommandResolver<AddIndexCommand> {
  @Override
  public Class<AddIndexCommand> getCommandClass() {
    return AddIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, AddIndexCommand command) {
    return new AddIndexOperation(ctx, command.column(), command.indexName());
  }
}
