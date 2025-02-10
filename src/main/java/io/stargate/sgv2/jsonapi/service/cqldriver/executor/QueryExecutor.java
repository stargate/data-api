package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
  private final OperationsConfig operationsConfig;

  /** CQLSession cache. */
  private final CQLSessionCache cqlSessionCache;

  @Inject
  public QueryExecutor(CQLSessionCache cqlSessionCache, OperationsConfig operationsConfig) {
    this.cqlSessionCache =
        Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
    this.operationsConfig =
        Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
  }

  /**
   * Execute read query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @param pagingState - In case of pagination, the paging state needs to be passed to fetch
   *     subsequent pages
   * @param pageSize - page size
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeRead(
      RequestContext requestContext,
      SimpleStatement simpleStatement,
      Optional<String> pagingState,
      int pageSize) {
    simpleStatement =
        simpleStatement
            .setPageSize(pageSize)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement.setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    return Uni.createFrom()
        .completionStage(cqlSessionCache.getSession(requestContext).executeAsync(simpleStatement));
  }

  /**
   * Execute count query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public CompletionStage<AsyncResultSet> executeCount(
      RequestContext requestContext, SimpleStatement simpleStatement) {
    simpleStatement =
        simpleStatement
            .setExecutionProfileName("count")
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    return cqlSessionCache.getSession(requestContext).executeAsync(simpleStatement);
  }

  /**
   * Execute count query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public CompletionStage<AsyncResultSet> executeEstimatedCount(
      RequestContext requestContext, SimpleStatement simpleStatement) {
    simpleStatement =
        simpleStatement.setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());

    return cqlSessionCache.getSession(requestContext).executeAsync(simpleStatement);
  }

  /**
   * Execute vector search query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @param pagingState - In case of pagination, the paging state needs to be passed to fetch
   *     subsequent pages
   * @param pageSize - page size
   * @return
   */
  public Uni<AsyncResultSet> executeVectorSearch(
      RequestContext requestContext,
      SimpleStatement simpleStatement,
      Optional<String> pagingState,
      int pageSize) {
    simpleStatement =
        simpleStatement
            .setPageSize(pageSize)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().vectorSearch());
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement.setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    return Uni.createFrom()
        .completionStage(cqlSessionCache.getSession(requestContext).executeAsync(simpleStatement));
  }

  /**
   * Execute write query with bound statement.
   *
   * @param statement - Bound statement with query and parameters. The table name used in the query
   *     must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeWrite(
      RequestContext requestContext, SimpleStatement statement) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession(requestContext)
                .executeAsync(
                    statement
                        .setIdempotent(true)
                        .setConsistencyLevel(
                            operationsConfig.queriesConfig().consistency().writes())
                        .setSerialConsistencyLevel(
                            operationsConfig.queriesConfig().serialConsistency())));
  }

  /**
   * Execute schema change query with bound statement for create ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeCreateSchemaChange(
      RequestContext requestContext, SimpleStatement boundStatement) {
    return executeSchemaChange(requestContext, boundStatement, "create");
  }

  /**
   * Execute schema change query with bound statement for drop ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeDropSchemaChange(
      RequestContext requestContext, SimpleStatement boundStatement) {
    return executeSchemaChange(requestContext, boundStatement, "drop");
  }

  /**
   * Execute schema change query with bound statement for truncate ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeTruncateSchemaChange(
      RequestContext requestContext, SimpleStatement boundStatement) {
    return executeSchemaChange(requestContext, boundStatement, "truncate");
  }

  private Uni<AsyncResultSet> executeSchemaChange(
      RequestContext requestContext, SimpleStatement boundStatement, String profile) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession(requestContext)
                .executeAsync(
                    boundStatement
                        .setExecutionProfileName(profile)
                        .setIdempotent(true)
                        .setSerialConsistencyLevel(
                            operationsConfig.queriesConfig().consistency().schemaChanges())))
        .onFailure(
            error ->
                error instanceof DriverTimeoutException
                    || error instanceof InvalidQueryException
                    || (error instanceof TruncateException
                        && "Failed to interrupt compactions".equals(error.getMessage())))
        .recoverWithUni(
            throwable -> {
              logger.error(
                  "Timeout/Invalid query executing schema change query : {}",
                  boundStatement.getQuery());
              SimpleStatement duplicate = SimpleStatement.newInstance(boundStatement.getQuery());
              return Uni.createFrom()
                  .item(throwable)
                  .onItem()
                  .delayIt()
                  .by(Duration.ofMillis(operationsConfig.databaseConfig().ddlRetryDelayMillis()))
                  .onItem()
                  .transformToUni(
                      v ->
                          Uni.createFrom()
                              .completionStage(
                                  cqlSessionCache
                                      .getSession(requestContext)
                                      .executeAsync(
                                          duplicate
                                              .setExecutionProfileName(profile)
                                              .setIdempotent(true)
                                              .setSerialConsistencyLevel(
                                                  operationsConfig
                                                      .queriesConfig()
                                                      .consistency()
                                                      .schemaChanges()))));
            })
        .onFailure(
            error ->
                error instanceof DriverTimeoutException
                    || error instanceof InvalidQueryException
                    || (error instanceof TruncateException
                        && "Failed to interrupt compactions".equals(error.getMessage())))
        .retry()
        .atMost(2);
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace
   * @param collectionName
   * @return
   */
  protected Uni<Optional<TableMetadata>> getSchema(
      RequestContext requestContext, String namespace, String collectionName) {
    try {
      var session = cqlSessionCache.getSession(requestContext);
      return Uni.createFrom()
          .completionStage(session.refreshSchemaAsync())
          .onItem()
          .transformToUni(
              v -> {
                KeyspaceMetadata keyspaceMetadata =
                    session.getMetadata().getKeyspaces().get(cqlIdentifierFromUserInput(namespace));
                if (keyspaceMetadata == null) {
                  return Uni.createFrom()
                      .failure(ErrorCodeV1.KEYSPACE_DOES_NOT_EXIST.toApiException("%s", namespace));
                }
                return Uni.createFrom()
                    .item(keyspaceMetadata.getTable(cqlIdentifierFromUserInput(collectionName)));
              });
    } catch (Exception e) {
      // TODO: this ^^ is a very wide error catch, confirm what it should actually be catching
      return Uni.createFrom().failure(e);
    }
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace - namespace
   * @param collectionName - collection name
   * @return TableMetadata
   */
  protected Uni<TableMetadata> getCollectionSchema(
      RequestContext requestContext, String namespace, String collectionName) {
    Optional<KeyspaceMetadata> keyspaceMetadata;
    if ((keyspaceMetadata =
            cqlSessionCache.getSession(requestContext).getMetadata().getKeyspace(namespace))
        .isPresent()) {
      Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(collectionName);
      if (tableMetadata.isPresent()) {
        return Uni.createFrom().item(tableMetadata.get());
      }
    }
    return Uni.createFrom().nullItem();
  }

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }

  public CQLSessionCache getCqlSessionCache() {
    return this.cqlSessionCache;
  }
}
