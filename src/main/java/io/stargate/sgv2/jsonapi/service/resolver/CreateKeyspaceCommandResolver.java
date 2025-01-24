package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.NamingValidationUtil.*;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateKeyspaceCommand;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
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
public class CreateKeyspaceCommandResolver
    extends CreateNamespaceKeyspaceCommandResolver<CreateKeyspaceCommand> {

  @Override
  public Class<CreateKeyspaceCommand> getCommandClass() {
    return CreateKeyspaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveDatabaseCommand(
      CommandContext<DatabaseSchemaObject> ctx, CreateKeyspaceCommand command) {

    if (!isValidName(command.name())) {
      throw SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.get(
          Map.of(
              "schemeType",
              KEYSPACE_SCHEMA_NAME,
              "nameLength",
              String.valueOf(NAME_LENGTH),
              "unsupportedSchemeName",
              command.name()));
    }

    String strategy =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategy()
            : null;

    Map<String, Integer> strategyOptions =
        (command.options() != null && command.options().replication() != null)
            ? command.options().replication().strategyOptions()
            : null;
    String replicationMap = getReplicationMap(strategy, strategyOptions);
    return new CreateKeyspaceOperation(command.name(), replicationMap);
  }
}
