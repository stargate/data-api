package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "stargate.debug")
public interface DebugModeConfig {

  @WithDefault("false")
  boolean enabled();
}
