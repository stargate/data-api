package io.stargate.sgv2.jsonapi.config.feature;

import io.smallrye.config.ConfigMapping;
import java.util.Map;
import java.util.Optional;

/** Configuration mapping for Data API Feature flags. */
@ConfigMapping(prefix = "stargate.feature")
public interface DataApiFeatureConfig {
  Map<DataApiFeatureFlag, Boolean> flags();

  default boolean isFeatureEnabled(DataApiFeatureFlag flag) {
    return isFeatureEnabled(flag, false);
  }

  default boolean isFeatureEnabled(DataApiFeatureFlag flag, boolean defaultValue) {
    return Optional.ofNullable(safeFlags().get(flag)).orElse(defaultValue);
  }

  default Map<DataApiFeatureFlag, Boolean> safeFlags() {
    return (flags() != null) ? flags() : Map.of();
  }
}
