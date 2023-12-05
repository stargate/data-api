package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "stargate.test")
public interface TestModeConfig {

  /** if jsonapi is running in tests (unit tests or integration tests), set true */
  @WithDefault("false")
  boolean inTest();
}
