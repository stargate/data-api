package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Configuration mapping for API Tables feature. */
@ConfigMapping(prefix = "stargate.tables")
public interface ApiTablesConfig {
  /** Setting that determines if the API Tables feature is enabled. */
  @WithDefault("false")
  boolean enabled();
}
