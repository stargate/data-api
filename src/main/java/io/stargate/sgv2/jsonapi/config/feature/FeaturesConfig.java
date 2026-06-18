package io.stargate.sgv2.jsonapi.config.feature;

import io.smallrye.config.ConfigMapping;
import java.util.Map;

/**
 * Configuration mapping for Data API Feature flags as read from main application configuration
 * (with possible property / sysenv overrides).
 *
 * <p>NOTE: actual keys in YAML or similar configuration file would be {@code
 * stargate.feature.<feature-name>}, where {@code <feature-name>} comes from {@link
 * ApiFeature#featureName()} (which is always lower-case). So, for example, to enable {@link
 * ApiFeature#TABLES} feature, one would use: {@code stargate.feature.flags.tables} as the key.
 */
@ConfigMapping(prefix = "stargate.feature")
public interface FeaturesConfig {
  // Quarkus/SmallRye Config won't accept use of `null` values, so we must bind
  // as Strings and only convert to Boolean when needed.
  Map<ApiFeature, String> flags();
}
