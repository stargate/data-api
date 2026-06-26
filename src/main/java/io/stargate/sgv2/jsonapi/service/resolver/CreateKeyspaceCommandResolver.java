package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.ApiOptionUtils.getOrDefault;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateKeyspaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/** Command resolver for {@link CreateKeyspaceCommand}. */
@ApplicationScoped
public class CreateKeyspaceCommandResolver
    extends KeyspaceDDLCommandResolver<CreateKeyspaceCommand> {

  @Override
  public Class<CreateKeyspaceCommand> getCommandClass() {
    return CreateKeyspaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, CreateKeyspaceCommand command) {

    var keyspaceName = keyspaceIdentifierForCreate(command.name());
    var replication =
        getOrDefault(command.options(), CreateKeyspaceCommand.Options::replication, null);

    String strategy = getOrDefault(replication, CreateKeyspaceCommand.Replication::strategy, null);

    Map<String, Integer> strategyOptions =
        getOrDefault(replication, CreateKeyspaceCommand.Replication::strategyOptions, null);

    validateStrategyOptions(strategy, strategyOptions);
    return new CreateKeyspaceOperation(keyspaceName, strategy, strategyOptions);
  }
}
