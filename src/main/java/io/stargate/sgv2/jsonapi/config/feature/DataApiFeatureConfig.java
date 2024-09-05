package io.stargate.sgv2.jsonapi.config.feature;

import io.smallrye.config.ConfigMapping;
import java.util.Map;

/**
 * Configuration mapping for Data API Feature flags as read from main application configuration
 * (with possible properety / sysenv overrides).
 */
@ConfigMapping(prefix = "stargate.feature")
public interface DataApiFeatureConfig {
  Map<DataApiFeatureFlag, Boolean> flags();

  default Boolean isFeatureEnabled(DataApiFeatureFlag ff) {
    return (flags() == null) ? null : flags().get(ff);
  }
}
