package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class CreateNamespaceKeyspaceCommandResolver<C extends Command>
    implements CommandResolver<C> {

  private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";

  // The driver writes NetworkTopologyStrategy map keys as CQL string literals without escaping
  // them.
  private static final Pattern VALID_DATA_CENTER_NAME = Pattern.compile("[^']+");

  protected static void validateStrategyOptions(
      String strategy, Map<String, Integer> strategyOptions) {
    if (!isNetworkTopologyStrategy(strategy) || strategyOptions == null) {
      return;
    }
    for (String dcName : strategyOptions.keySet()) {
      checkDataCenterName(dcName);
    }
  }

  private static boolean isNetworkTopologyStrategy(String strategy) {
    return NETWORK_TOPOLOGY_STRATEGY.equalsIgnoreCase(strategy);
  }

  private static void checkDataCenterName(String name) {
    if (name == null || !VALID_DATA_CENTER_NAME.matcher(name).matches()) {
      throw SchemaException.Code.UNSUPPORTED_REPLICATION_DATA_CENTER_NAME.get(
          Map.of("unsupportedDataCenterName", ErrorTemplate.replaceIfNull(name)));
    }
  }
}
