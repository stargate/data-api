package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.DropKeyspaceOperation;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Command resolver for {@link DropNamespaceCommand}. Resolve a {@link DropNamespaceCommand} to a
 * {@link DropKeyspaceOperation}
 */
@ApplicationScoped
public class DropNamespaceCommandResolver implements CommandResolver<DropNamespaceCommand> {

  /** {@inheritDoc} */
  @Override
  public Class<DropNamespaceCommand> getCommandClass() {
    return DropNamespaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, DropNamespaceCommand command) {
    return new DropKeyspaceOperation(command.name());
  }
}
