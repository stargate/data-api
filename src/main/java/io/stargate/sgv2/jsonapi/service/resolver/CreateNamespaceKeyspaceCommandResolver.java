package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class CreateNamespaceKeyspaceCommandResolver<C extends Command>
    implements CommandResolver<C> {

  // default if omitted
  private static final String DEFAULT_REPLICATION_MAP =
      "{'class': 'SimpleStrategy', 'replication_factor': 1}";

  // Allowlist for datacenter names interpolated into the CQL replication map.
  // Permits common Cassandra/cloud DC naming (alphanumeric, underscore, hyphen) while
  // rejecting any character that could break out of the single-quoted CQL string literal.
  private static final Pattern VALID_DATA_CENTER_NAME = Pattern.compile("[A-Za-z0-9_\\-]+");
  private static final int MAX_DATA_CENTER_NAME_LENGTH = 48;

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
        var dcName = checkDataCenterName(dcEntry.getKey());
        map.append(", '%s': %d".formatted(dcName, dcEntry.getValue()));
      }
    }
    map.append("}");
    return map.toString();
  }

  private static String checkDataCenterName(String name) {
    if (name == null
        || name.isEmpty()
        || name.length() > MAX_DATA_CENTER_NAME_LENGTH
        || !VALID_DATA_CENTER_NAME.matcher(name).matches()) {
      throw SchemaException.Code.UNSUPPORTED_REPLICATION_DATA_CENTER_NAME.get(
          Map.of(
              "maxNameLength",
              String.valueOf(MAX_DATA_CENTER_NAME_LENGTH),
              "unsupportedDataCenterName",
              ErrorTemplate.replaceIfNull(name)));
    }
    return name;
  }

  private static String simpleStrategyMap(Map<String, Integer> strategyOptions) {
    if (null == strategyOptions || strategyOptions.isEmpty()) {
      return DEFAULT_REPLICATION_MAP;
    }

    Integer replicationFactor = strategyOptions.getOrDefault("replication_factor", 1);
    return "{'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "}";
  }
}
