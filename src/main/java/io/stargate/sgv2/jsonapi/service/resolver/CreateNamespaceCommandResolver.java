package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.CreateKeyspaceOperation;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;

/**
 * Command resolver for {@link CreateNamespaceCommand}. Resolves to a {@link
 * CreateKeyspaceOperation} which builds the {@code CREATE KEYSPACE} statement via the driver's
 * {@code SchemaBuilder}.
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

    var keyspaceName = NamingRules.KEYSPACE.checkRule(command.name());

    String strategy =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategy()
            : null;

    Map<String, Integer> strategyOptions =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategyOptions()
            : null;

    validateStrategyOptions(strategy, strategyOptions);
    return new CreateKeyspaceOperation(keyspaceName, strategy, strategyOptions);
  }
}
