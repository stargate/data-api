package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.service.resolver.KeyspaceCommandResolverSupport.keyspaceIdentifierForDrop;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.DropKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;

/** Command resolver for {@link DropNamespaceCommand}. */
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
    return new DropKeyspaceOperation(keyspaceIdentifierForDrop(command.name()));
  }
}
