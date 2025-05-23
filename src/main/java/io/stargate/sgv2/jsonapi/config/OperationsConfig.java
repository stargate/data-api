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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/** Configuration for the operation execution. */
@ConfigMapping(prefix = "stargate.jsonapi.operations")
public interface OperationsConfig {
  /** Defines the default max size of filter fields. */
  int DEFAULT_MAX_FILTER_SIZE = 64;

  /**
   * Defines the default maximum documents to insert setting for {@code InsertMany} command;
   * defaults to 100
   */
  public static final int DEFAULT_MAX_DOCUMENT_INSERT_COUNT = 100;

  /**
   * @return Defines the default document page size, defaults to <code>20</code>.
   */
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
   * @return Flag to enable vectorization using embedding-gateway.
   */
  @WithDefault("false")
  boolean enableEmbeddingGateway();

  /**
   * @return Flag to extend error response with additional information.
   */
  @WithDefault("true")
  boolean extendError();

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
  @WithDefault("" + DEFAULT_MAX_DOCUMENT_INSERT_COUNT)
  int maxDocumentInsertCount();

  /**
   * @return Defines the max size of filter fields, defaults to {@code 64}. (note: this does not
   *     count the fields in '$operation' such as $in, $all)
   */
  @Positive
  @WithDefault("" + DEFAULT_MAX_FILTER_SIZE)
  int maxFilterObjectProperties();

  /**
   * @return Maximum size of values array that can be sent in $in/$nin operator
   */
  @Max(100)
  @Positive
  @WithDefault("100")
  int maxInOperatorValueSize();

  /**
   * @return Maximum size of documents returned with vector search, max value supported in cassandra
   *     is 1000 <code>1000</code> command.
   */
  @Max(1000)
  @Positive
  @WithDefault("1000")
  int maxVectorSearchLimit();

  /**
   * @return Maximum size of keys read from database to return count, Setting it to -1 will use
   *     Cassandra's count function. Default is <code>1000</code>.
   */
  @WithDefault("1000")
  int maxCountLimit();

  /**
   * @return Boolean flag to enable astra index guardrail too many indexes rollback
   */
  @WithDefault("true")
  boolean tooManyIndexesRollbackEnabled();

  /**
   * Optional string that is the case-insensitive user agent string that will be used to identify if
   * a request is from an SLA checker. Requests from SLA checkers may be treated differently for
   * features such as caching sessions.(Empty string is treated as unset / null)
   */
  @WithDefault("")
  @Nullable
  Optional<String> slaUserAgent();

  /**
   * @return Defines the default page size for count operation, having separate from
   *     `defaultPageSize` config because count will read more keys per page, defaults to <code>100
   *     </code>.
   */
  @Max(500)
  @Positive
  @WithDefault("100")
  int defaultCountPageSize();

  @NotNull
  @Valid
  LwtConfig lwt();

  /** Configuration setup for the Light-weight transactions. */
  interface LwtConfig {

    /**
     * @return Defines the maximum retry for lwt failure <code>3</code>.
     */
    @Max(5)
    @Positive
    @WithDefault("3")
    int retries();
  }

  /** Cassandra/AstraDB related configurations. */
  @NotNull
  @Valid
  DatabaseConfig databaseConfig();

  interface DatabaseConfig {

    /**
     * The type of backend DB to connect to, this drives decisions like using the cassandraEndPoints
     */
    @WithDefault(DatabaseType.Constants.CASSANDRA)
    @WithConverter(DatabaseType.DatabaseTypeConverter.class)
    DatabaseType type();

    /**
     * Username when connecting to cassandra database (when type is {@link DatabaseType#CASSANDRA})
     * and fixedToken is used
     */
    @Nullable
    @WithDefault("cassandra")
    String userName();

    /**
     * Password when connecting to cassandra database (when type is {@link DatabaseType#CASSANDRA})
     * and fixedToken is used
     */
    @Nullable
    @WithDefault("cassandra")
    String password();

    /**
     * Fixed Token used for Integration Test authentication. When set, all tokens must match this
     * value and the userName and password from this config are always used for the db credentials
     */
    Optional<String> fixedToken();

    /** Cassandra contact points (when type is {@link DatabaseType#CASSANDRA}) */
    @Nullable
    @WithDefault("127.0.0.1")
    List<String> cassandraEndPoints();

    /** Cassandra port (when type is {@link DatabaseType#CASSANDRA}) */
    @WithDefault("9042")
    int cassandraPort();

    /** Local datacenter that the driver must be configured with */
    @NotNull
    @WithDefault("datacenter1")
    String localDatacenter();

    /** Time to live for CQLSession in cache in seconds, that are not from the slaUserAgent. */
    @WithDefault("300")
    long sessionCacheTtlSeconds();

    /**
     * Time to live for CQLSession created because of a request from with the SLA user-agent in
     * cache in seconds.
     */
    @WithDefault("10")
    long slaSessionCacheTtlSeconds();

    /** Maximum number of CQLSessions in cache. */
    @WithDefault("50")
    int sessionCacheMaxSize();

    /** DDL query retry wait in millis. */
    @WithDefault("1000")
    int ddlRetryDelayMillis();

    /** Create table cool off period before create index . */
    @WithDefault("2000")
    int ddlDelayMillis();

    /** Maximum retry attempt for ddl command. */
    @WithDefault("2")
    int ddlRetries();
  }

  /** Query consistency related configs. */
  @NotNull
  @Valid
  QueriesConfig queriesConfig();

  interface QueriesConfig {

    /**
     * @return Settings for the consistency level.
     */
    @Valid
    ConsistencyConfig consistency();

    /**
     * @return Serial Consistency for queries.
     */
    @WithDefault("LOCAL_SERIAL")
    @WithConverter(ConsistencyLevelConverter.class)
    ConsistencyLevel serialConsistency();

    /**
     * @return Settings for the consistency level.
     */
    interface ConsistencyConfig {

      /**
       * @return Consistency for queries making schema changes.
       */
      @WithDefault("LOCAL_QUORUM")
      @NotNull
      @WithConverter(ConsistencyLevelConverter.class)
      ConsistencyLevel schemaChanges();

      /**
       * @return Consistency for queries writing the data.
       */
      @WithDefault("LOCAL_QUORUM")
      @NotNull
      @WithConverter(ConsistencyLevelConverter.class)
      ConsistencyLevel writes();

      /**
       * @return Consistency for queries reading the data.
       */
      @WithDefault("LOCAL_QUORUM")
      @NotNull
      @WithConverter(ConsistencyLevelConverter.class)
      ConsistencyLevel reads();

      /**
       * @return Consistency for vector search queries.
       */
      @WithDefault("LOCAL_ONE")
      @NotNull
      @WithConverter(ConsistencyLevelConverter.class)
      ConsistencyLevel vectorSearch();
    }
  }

  /**
   * @return Flag to enable server side vectorization.
   */
  @WithDefault("false")
  boolean vectorizeEnabled();

  /** Offline mode configuration. */
  @NotNull
  @Valid
  OfflineModeConfig offlineModeConfig();

  interface OfflineModeConfig {

    @WithDefault("1000")
    int maxDocumentInsertCount();
  }

  @NotNull
  @WithDefault("0,50,100")
  @WithConverter(IntConfigWithBoundsConverter.class)
  IntConfigWithBounds hybridSearchVectorLimit();

  @NotNull
  @WithDefault("0,50,100")
  @WithConverter(IntConfigWithBoundsConverter.class)
  IntConfigWithBounds hybridSearchLexicalLimit();
}
