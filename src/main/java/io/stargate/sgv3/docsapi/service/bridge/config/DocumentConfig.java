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

package io.stargate.sgv3.docsapi.service.bridge.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import javax.validation.constraints.Max;
import javax.validation.constraints.Positive;

/** Configuration for the documents. */
@ConfigMapping(prefix = "stargate.document")
public interface DocumentConfig {

  /** @return Defines the maximum document page size, defaults to <code>100</code>. */
  @Max(500)
  @Positive
  @WithDefault("100")
  int maxPageSize();

  /** @return Defines the default document page size, defaults to <code>100</code>. */
  @Max(500)
  @Positive
  @WithDefault("20")
  int defaultPageSize();

  /**
   * @return Defines the maximum limit of document that can be returned for a request, defaults to
   *     <code>1000</code>.
   */
  @Max(Integer.MAX_VALUE)
  @Positive
  @WithDefault("1000")
  int maxLimit();
}
