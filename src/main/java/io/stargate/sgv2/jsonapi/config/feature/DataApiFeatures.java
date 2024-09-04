package io.stargate.sgv2.jsonapi.config.feature;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Accessor for combined state of feature flags; typically based on static configuration (with its
 * overrides) and possible per-request settings.
 */
public class DataApiFeatures {
  private final Map<DataApiFeatureFlag, Boolean> fromConfig;

  DataApiFeatures(Map<DataApiFeatureFlag, Boolean> fromConfig) {
    this.fromConfig = (fromConfig == null) ? Collections.emptyMap() : fromConfig;
  }

  public static DataApiFeatures empty() {
    return new DataApiFeatures(Collections.emptyMap());
  }

  public static DataApiFeatures fromConfigOnly(DataApiFeatureConfig config) {
    return new DataApiFeatures(config.flags());
  }

  public boolean isFeatureEnabled(DataApiFeatureFlag flag) {
    return isFeatureEnabled(flag, false);
  }

  public boolean isFeatureEnabled(DataApiFeatureFlag flag, boolean defaultValue) {
    return Optional.ofNullable(fromConfig.get(flag)).orElse(defaultValue);
  }
}
