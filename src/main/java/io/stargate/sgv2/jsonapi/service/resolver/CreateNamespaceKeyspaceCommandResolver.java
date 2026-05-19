package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class CreateNamespaceKeyspaceCommandResolver<C extends Command>
    implements CommandResolver<C> {

  private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";

  // Allowlist for datacenter names. The driver's SchemaBuilder.withNetworkTopologyStrategy(Map)
  // does NOT escape map keys (see OptionsUtils.extractOptionValue in java-driver-query-builder),
  // so a hostile DC-name key passed to the driver would still produce broken CQL. This allowlist
  // is therefore the actual security control for the replication map, not just API policy —
  // do not remove without first ensuring DC names are escaped before reaching the driver.
  private static final Pattern VALID_DATA_CENTER_NAME = Pattern.compile("[A-Za-z0-9_\\-]+");
  private static final int MAX_DATA_CENTER_NAME_LENGTH = 48;

  /**
   * Validate datacenter-name map keys when the strategy is {@code NetworkTopologyStrategy}. For
   * other strategies the {@code strategyOptions} map is interpreted by the driver as simple-
   * strategy options (e.g. {@code replication_factor}) and is not validated here.
   */
  protected static void validateStrategyOptions(
      String strategy, Map<String, Integer> strategyOptions) {
    if (!NETWORK_TOPOLOGY_STRATEGY.equals(strategy) || strategyOptions == null) {
      return;
    }
    for (String dcName : strategyOptions.keySet()) {
      checkDataCenterName(dcName);
    }
  }

  private static void checkDataCenterName(String name) {
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
  }
}
