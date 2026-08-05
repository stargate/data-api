package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;


@ConfigMapping(prefix = "stargate.jsonapi.deployment-marker")
public interface DeploymentMarkerConfig {


  @WithDefault("false")
  boolean enabled();

  Optional<String> value();
}
