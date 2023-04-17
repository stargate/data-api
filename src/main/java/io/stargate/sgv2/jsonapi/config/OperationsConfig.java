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
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

/** Configuration for the operation execution. */
@ConfigMapping(prefix = "stargate.jsonapi.operations")
public interface OperationsConfig {

  /** @return Defines the default document page size, defaults to <code>20</code>. */
  @Max(500)
  @Positive
  @WithDefault("20")
  int defaultPageSize();

  /**
   * @return Defines the default document page size for sorting, having separate config because sort
   *     will more rows in per page, defaults to <code>100</code>.
   */
  @Max(500)
  @Positive
  @WithDefault("100")
  int defaultSortPageSize();

  /**
   * @return Defines the maximum limit of document read to perform in memory sorting <code>10000
   *     </code>.
   */
  @Max(10000)
  @Positive
  @WithDefault("10000")
  int maxDocumentSortCount();

  /**
   * @return Defines the maximum limit of document that can be deleted for a request, defaults to
   *     <code>20</code>.
   */
  @Max(100)
  @Positive
  @WithDefault("20")
  int maxDocumentDeleteCount();

  /**
   * @return Defines the maximum limit of document that can be updated for a request, defaults to
   *     <code>20</code>.
   */
  @Max(100)
  @Positive
  @WithDefault("20")
  int maxDocumentUpdateCount();

  /**
   * @return Maximum amount of documents that can be inserted using <code>insertMany</code> command.
   */
  @Max(100)
  @Positive
  @WithDefault("20")
  int maxDocumentInsertCount();

  @NotNull
  @Valid
  LwtConfig lwt();

  /** Configuration setup for the Light-weight transactions. */
  interface LwtConfig {

    /** @return Defines the maximum retry for lwt failure <code>3</code>. */
    @Max(5)
    @Positive
    @WithDefault("3")
    int retries();
  }
}
