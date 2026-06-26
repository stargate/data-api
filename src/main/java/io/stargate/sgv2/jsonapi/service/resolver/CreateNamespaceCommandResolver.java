package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/** Command resolver for {@link CreateNamespaceCommand}. */
@ApplicationScoped
public class CreateNamespaceCommandResolver
    extends KeyspaceDDLCommandResolver<CreateNamespaceCommand> {

  @Override
  public Class<CreateNamespaceCommand> getCommandClass() {
    return CreateNamespaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, CreateNamespaceCommand command) {

    var keyspaceName = keyspaceIdentifierForCreate(command.name());
    var replication =
        getOrDefault(command.options(), CreateNamespaceCommand.Options::replication, null);

    String strategy = getOrDefault(replication, CreateNamespaceCommand.Replication::strategy, null);

    Map<String, Integer> strategyOptions =
        getOrDefault(replication, CreateNamespaceCommand.Replication::strategyOptions, null);

    validateStrategyOptions(strategy, strategyOptions);
    return new CreateKeyspaceOperation(keyspaceName, strategy, strategyOptions);
  }
}
