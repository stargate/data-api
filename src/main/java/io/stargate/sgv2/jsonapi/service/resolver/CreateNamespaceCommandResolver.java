package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/**
 * Command resolver for {@link CreateNamespaceCommand}. Responsible for creating the replication
 * map. Resolve a {@link CreateNamespaceCommand} to a {@link CreateKeyspaceOperation}
 */
@ApplicationScoped
public class CreateNamespaceCommandResolver
    extends CreateNamespaceKeyspaceCommandResolver<CreateNamespaceCommand> {

  @Override
  public Class<CreateNamespaceCommand> getCommandClass() {
    return CreateNamespaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, CreateNamespaceCommand command) {

    final var name = validateSchemaName(command.name(), NamingRules.KEYSPACE);

    String strategy =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategy()
            : null;

    Map<String, Integer> strategyOptions =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategyOptions()
            : null;

    String replicationMap = getReplicationMap(strategy, strategyOptions);
    return new CreateKeyspaceOperation(name, replicationMap);
  }
}
