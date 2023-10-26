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
import jakarta.validation.constraints.Positive;

/** Configuration for limits that apply to Databases. */
@ConfigMapping(prefix = "stargate.database.limits")
public interface DatabaseLimitsConfig {
  int DEFAULT_MAX_COLLECTIONS = 5;

  /**
   * @return Defines maximum Collections allowed to be created per Database. Defaults to <code>5
   *     </code> due to underlying Cassandra indexing limitations.
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_COLLECTIONS)
  int maxCollections();
}
