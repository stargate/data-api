package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateKeyspaceCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/**
 * Command resolver for {@link CreateKeyspaceCommand}. Responsible for creating the replication map.
 * Resolve a {@link CreateKeyspaceCommand} to a {@link CreateKeyspaceOperation}
 */
@ApplicationScoped
public class CreateKeyspaceCommandResolver extends CreateNamespaceKeyspaceCommandResolver<CreateKeyspaceCommand> {


  @Override
  public Class<CreateKeyspaceCommand> getCommandClass() {
    return CreateKeyspaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, CreateKeyspaceCommand command) {
    String replicationMap = getReplicationMap(command.options());
    return new CreateKeyspaceOperation(command.name(), replicationMap);
  }

}
