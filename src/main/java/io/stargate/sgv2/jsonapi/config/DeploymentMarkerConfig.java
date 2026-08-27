package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/** Deployment marker configuration, read from the per-deployment environment. */
@ConfigMapping(prefix = "stargate.jsonapi.deployment.env")
public interface DeploymentMarkerConfig {

  @WithDefault("false")
  boolean enabled();

  Optional<String> deployCloud();

  Optional<String> deploy_region();

  @WithDefault("0")
  int value();
}
