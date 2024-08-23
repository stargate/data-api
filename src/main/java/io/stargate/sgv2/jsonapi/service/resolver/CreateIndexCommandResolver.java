package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.CreateIndexOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolver for the {@link CreateIndexCommand}. */
@ApplicationScoped
public class CreateIndexCommandResolver implements CommandResolver<CreateIndexCommand> {
  @Override
  public Class<CreateIndexCommand> getCommandClass() {
    return CreateIndexCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, CreateIndexCommand command) {
    return new CreateIndexOperation(ctx, command.column(), command.indexName());
  }
}
