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

import static io.stargate.sgv2.api.common.config.constants.LoggingConstants.*;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;
import java.util.Set;

/** Extra, JSON API command level logging related configurations. */
@ConfigMapping(prefix = "stargate.jsonapi.logging")
public interface CommandLevelLoggingConfig {

  /**
   * @return If request info logging is enabled.
   */
  @WithDefault("false")
  boolean enabled();

  /**
   * @return If only requests with errors should be logged.
   */
  @WithDefault("true")
  boolean onlyResultsWithErrors();

  /**
   * @return Set of tenants for which the request info should be logged.
   */
  @WithDefault(ALL_TENANTS)
  Optional<Set<String>> enabledTenants();
}
