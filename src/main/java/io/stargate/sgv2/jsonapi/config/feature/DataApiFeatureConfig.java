package io.stargate.sgv2.jsonapi.config.feature;

import io.smallrye.config.ConfigMapping;
import java.util.Map;

/** Configuration mapping for Data API Feature flags. */
@ConfigMapping(prefix = "stargate.feature")
public interface DataApiFeatureConfig {
  Map<DataApiFeatureFlag, Boolean> flags();
}
