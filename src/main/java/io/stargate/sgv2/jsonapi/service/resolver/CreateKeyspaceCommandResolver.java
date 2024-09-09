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
public class CreateKeyspaceCommandResolver implements CommandResolver<CreateKeyspaceCommand> {

  // default if omitted
  private static final String DEFAULT_REPLICATION_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor': 1}";

  /** {@inheritDoc} */
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

  // resolve the replication map
  private String getReplicationMap(CreateKeyspaceCommand.Options options) {
    if (null == options) {
      return DEFAULT_REPLICATION_MAP;
    }

    // TODO REMOVE THE OPTION TO PASS REPLICATION STRATEGY!!!!
    CreateKeyspaceCommand.Replication replication = options.replication();
    if ("NetworkTopologyStrategy".equals(replication.strategy())) {
      return networkTopologyStrategyMap(replication);
    } else {
      return simpleStrategyMap(replication);
    }
  }

  private static String networkTopologyStrategyMap(CreateKeyspaceCommand.Replication replication) {
    Map<String, Integer> options = replication.strategyOptions();

    StringBuilder map = new StringBuilder("{'class': 'NetworkTopologyStrategy'");
    if (null != options) {
      for (Map.Entry<String, Integer> dcEntry : options.entrySet()) {
        map.append(", '%s': %d".formatted(dcEntry.getKey(), dcEntry.getValue()));
      }
    }
    map.append("}");
    return map.toString();
  }

  private static String simpleStrategyMap(CreateKeyspaceCommand.Replication replication) {
    Map<String, Integer> options = replication.strategyOptions();
    if (null == options || options.isEmpty()) {
      return DEFAULT_REPLICATION_MAP;
    }

    Integer replicationFactor = options.getOrDefault("replication_factor", 1);
    return "{'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "}";
  }
}
