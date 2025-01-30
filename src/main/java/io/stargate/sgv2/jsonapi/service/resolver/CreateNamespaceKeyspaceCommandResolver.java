package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import java.util.Map;

public abstract class CreateNamespaceKeyspaceCommandResolver<C extends Command>
    implements CommandResolver<C> {

  // default if omitted
  private static final String DEFAULT_REPLICATION_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor': 1}";

  // resolve the replication map
  String getReplicationMap(String strategy, Map<String, Integer> strategyOptions) {
    if (strategy == null && strategyOptions == null) {
      return DEFAULT_REPLICATION_MAP;
    }
    if ("NetworkTopologyStrategy".equals(strategy)) {
      return networkTopologyStrategyMap(strategyOptions);
    } else {
      return simpleStrategyMap(strategyOptions);
    }
  }

  private static String networkTopologyStrategyMap(Map<String, Integer> strategyOptions) {
    StringBuilder map = new StringBuilder("{'class': 'NetworkTopologyStrategy'");
    if (null != strategyOptions) {
      for (Map.Entry<String, Integer> dcEntry : strategyOptions.entrySet()) {
        map.append(", '%s': %d".formatted(dcEntry.getKey(), dcEntry.getValue()));
      }
    }
    map.append("}");
    return map.toString();
  }

  private static String simpleStrategyMap(Map<String, Integer> strategyOptions) {
    if (null == strategyOptions || strategyOptions.isEmpty()) {
      return DEFAULT_REPLICATION_MAP;
    }

    Integer replicationFactor = strategyOptions.getOrDefault("replication_factor", 1);
    return "{'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "}";
  }
}
