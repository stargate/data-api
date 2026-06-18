package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropKeyspaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.DropKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;

/** Command resolver for {@link DropKeyspaceCommand}. */
@ApplicationScoped
public class DropKeyspaceCommandResolver implements CommandResolver<DropKeyspaceCommand> {

  /** {@inheritDoc} */
  @Override
  public Class<DropKeyspaceCommand> getCommandClass() {
    return DropKeyspaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, DropKeyspaceCommand command) {
    return new DropKeyspaceOperation(command.name());
  }
}
