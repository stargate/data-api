package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateDatabaseCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateDatabaseOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;

/**
 * Command resolver for {@link CreateDatabaseCommand}. Responsible for creating the replication map.
 */
@ApplicationScoped
public class CreateDatabaseResolver implements CommandResolver<CreateDatabaseCommand> {

  // default if omitted
  private static final String DEFAULT_REPLICATION_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor': 1 }";

  /** {@inheritDoc} */
  @Override
  public Class<CreateDatabaseCommand> getCommandClass() {
    return CreateDatabaseCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, CreateDatabaseCommand command) {
    String replicationMap = getReplicationMap(command.options());
    return new CreateDatabaseOperation(command.name(), replicationMap);
  }

  // resolve the replication map
  private String getReplicationMap(CreateDatabaseCommand.Options options) {
    if (null == options) {
      return DEFAULT_REPLICATION_MAP;
    }

    CreateDatabaseCommand.Replication replication = options.replication();
    if ("NetworkTopologyStrategy".equals(replication.strategy())) {
      return networkTopologyStrategyMap(replication);
    } else {
      return simpleStrategyMap(replication);
    }
  }

  private static String networkTopologyStrategyMap(CreateDatabaseCommand.Replication replication) {
    Map<String, Integer> options = replication.strategyOptions();

    StringBuilder map = new StringBuilder("{'class': 'NetworkTopologyStrategy'");
    if (null != options) {
      for (Map.Entry<String, Integer> dcEntry : options.entrySet()) {
        map.append(", \"%s\": %d".formatted(dcEntry.getKey(), dcEntry.getValue()));
      }
    }
    map.append("}");
    return map.toString();
  }

  private static String simpleStrategyMap(CreateDatabaseCommand.Replication replication) {
    Map<String, Integer> options = replication.strategyOptions();
    if (null == options || options.isEmpty()) {
      return DEFAULT_REPLICATION_MAP;
    }

    Integer replicationFactor = options.getOrDefault("replication_factor", 1);
    return "{'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + " }";
  }
}
