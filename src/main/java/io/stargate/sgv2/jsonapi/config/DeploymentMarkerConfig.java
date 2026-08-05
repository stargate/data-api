package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

@ConfigMapping(prefix = "stargate.jsonapi.deployment")
public interface DeploymentMarkerConfig {

  @WithDefault("false")
  boolean enabled();

  Optional<String> deployCloud();

  Optional<String> deploy_region();

  @WithDefault("0")
  int value();
}
