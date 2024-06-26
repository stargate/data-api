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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.constraints.Positive;

/** Configuration for limits that apply to Databases. */
@StaticInitSafe
@ConfigMapping(prefix = "stargate.database.limits")
public interface DatabaseLimitsConfig {
  /**
   * Default setting for {@link #maxCollections()}}: maximum number of Collections allowed to be
   * created per Database, regardless of whether enough Indexes are available.
   */
  int DEFAULT_MAX_COLLECTIONS = 10;

  /**
   * Default setting for {@link #indexesNeededPerCollection()}: number of SAIs needed to create one
   * new Collection. Uses upper limit across all Collection types, so slightly conservative.
   */
  int DEFAULT_INDEXES_NEEDED_PER_COLLECTION = 10;

  /**
   * Default setting for {@link #indexesAvailablePerDatabase()}: number of SAIs per Database that
   * can be created (and needed for Collection creation)
   */
  int DEFAULT_INDEXES_AVAILABLE_PER_DATABASE = 100;

  /**
   * @return Defines maximum Collections allowed to be created per Database. Defaults to {@link
   *     #DEFAULT_MAX_COLLECTIONS}.
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_COLLECTIONS)
  int maxCollections();

  /**
   * @return Defines how many indexes we need to be able to add to create a new Collection. Defaults
   *     to {@link #DEFAULT_INDEXES_NEEDED_PER_COLLECTION}.
   */
  @Positive
  @WithDefault("" + DEFAULT_INDEXES_NEEDED_PER_COLLECTION)
  int indexesNeededPerCollection();

  /**
   * @return Indicates how many indexes are available per Database: needed to calculate number of
   *     Indexes that can still be created (and thus Collections). Defaults to {@link
   *     #DEFAULT_INDEXES_AVAILABLE_PER_DATABASE}.
   */
  @Positive
  @WithDefault("" + DEFAULT_INDEXES_AVAILABLE_PER_DATABASE)
  int indexesAvailablePerDatabase();
}
