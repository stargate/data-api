/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Configuration for opt-in GPU-plane token pre-validation.
 *
 * <p>When enabled, the API validates the inbound {@code Authorization} header against DataStax
 * Astra control-plane endpoints before the request reaches Cassandra. Disabled by default; OSS
 * deployments are unaffected.
 *
 * <p>Replaces the standalone {@code gpu-api-gateway} auth Lambda; see the original Go
 * implementation in the {@code cloud-helm-charts} repo for the source of these flows.
 */
@ConfigMapping(prefix = "stargate.jsonapi.gpu-plane-auth")
public interface GpuPlaneAuthConfig {

  /** Whether GPU-plane pre-validation is active. Off by default. */
  @WithDefault("false")
  boolean enabled();

  /**
   * Basic-auth username for the Astra Insights Plane (used on the {@code /v2/token/authorize}
   * call). Empty when the feature is disabled.
   */
  Optional<String> insightsPlaneUsername();

  /** Basic-auth password counterpart to {@link #insightsPlaneUsername()}. */
  Optional<String> insightsPlanePassword();
}
