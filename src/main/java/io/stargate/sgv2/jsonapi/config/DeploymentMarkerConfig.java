package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Configuration for the deployment marker: an opt-in, inert string that the application echoes once
 * at startup.
 *
 * <p>The marker drives no behaviour of its own, it is only an observation point for deployment
 * drills. The value is handed to the pod as an environment variable, and when {@link #enabled()} is
 * {@code true} it is written to the startup log by {@link io.stargate.sgv2.jsonapi.JsonApiStartUp}.
 * Left at the {@code false} default, no marker line is logged and no request handling changes.
 *
 * <p>As for any {@code @ConfigMapping}, SmallRye Config also reads these properties from
 * environment variables, which is how a Helm chart would supply them:
 *
 * <pre>
 * STARGATE_JSONAPI_DEPLOYMENT_MARKER_ENABLED=true
 * STARGATE_JSONAPI_DEPLOYMENT_MARKER_VALUE=2026-08-04.1
 * </pre>
 */
@ConfigMapping(prefix = "stargate.jsonapi.deployment-marker")
public interface DeploymentMarkerConfig {

  /**
   * @return If the deployment marker should be logged at startup.
   */
  @WithDefault("false")
  boolean enabled();

  /**
   * Deliberately not annotated {@code @WithDefault("")}: SmallRye's built-in String converter reads
   * an empty default as {@code null}, and a non-optional mapping property then fails startup with
   * {@code SRCFG00040}. {@link Optional} is also how the neighbouring config interfaces express
   * "may be unset".
   *
   * @return The marker to log, such as a release tag or a rollout id. Only read when {@link
   *     #enabled()} is {@code true}, and logged as an empty marker when not set.
   */
  Optional<String> value();
}
