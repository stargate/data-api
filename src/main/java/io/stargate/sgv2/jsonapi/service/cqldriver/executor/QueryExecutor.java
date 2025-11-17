package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.DBTraceMessages;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.ErrorObjectV2Constants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is legacy class from the first versions of the API, this class is now created in {@link
 * io.stargate.sgv2.jsonapi.service.operation.Operation#execute(RequestContext, QueryExecutor)} for
 * backwards compatibility. From there is passed to the operation and used to execute.
 *
 * <p>It is no longer a bean and should not be injected.
 */
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
  private final OperationsConfig operationsConfig;

  /** CQLSession cache. */
  private final CQLSessionCache cqlSessionCache;

  private final RequestTracing requestTracing;

  public QueryExecutor(CQLSessionCache cqlSessionCache, OperationsConfig operationsConfig) {
    this(cqlSessionCache, operationsConfig, RequestTracing.NO_OP);
  }

  public QueryExecutor(
      CQLSessionCache cqlSessionCache,
      OperationsConfig operationsConfig,
      RequestTracing requestTracing) {
    this.cqlSessionCache =
        Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
    this.operationsConfig =
        Objects.requireNonNull(operationsConfig, "operationsConfig must not be null");
    this.requestTracing = Objects.requireNonNull(requestTracing, "requestTracing must not be null");
  }

  private Uni<AsyncResultSet> executeAsync(
      RequestContext requestContext, SimpleStatement statement) {

    var stmtWithTracing =
        requestTracing.enabled() != statement.isTracing()
            ? statement.setTracing(requestTracing.enabled())
            : statement;

    DBTraceMessages.executingStatement(
        requestTracing, stmtWithTracing, "Executing statement for non task based operation");

    return cqlSessionCache
        .getSession(requestContext)
        .flatMap(
            session ->
                Uni.createFrom().completionStage(() -> session.executeAsync(stmtWithTracing)))
        .onItem()
        .call(
            asyncResultSet ->
                DBTraceMessages.maybeCqlTrace(
                    requestTracing,
                    asyncResultSet,
                    "Statement trace for non task based operation"));
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
    return executeAsync(requestContext, simpleStatement);
  }

  /**
   * Execute count query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeCount(
      RequestContext requestContext, SimpleStatement simpleStatement) {
    simpleStatement =
        simpleStatement
            .setExecutionProfileName("count")
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    return executeAsync(requestContext, simpleStatement);
  }

  /**
   * Execute count query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeEstimatedCount(
      RequestContext requestContext, SimpleStatement simpleStatement) {
    simpleStatement =
        simpleStatement.setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());

    return executeAsync(requestContext, simpleStatement);
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

    return executeAsync(requestContext, simpleStatement);
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

    var stmtToExec =
        statement
            .setIdempotent(true)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().writes())
            .setSerialConsistencyLevel(operationsConfig.queriesConfig().serialConsistency());

    return executeAsync(requestContext, stmtToExec);
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

    var stmtToExec =
        boundStatement
            .setExecutionProfileName(profile)
            .setIdempotent(true)
            .setSerialConsistencyLevel(
                operationsConfig.queriesConfig().consistency().schemaChanges());

    return executeAsync(requestContext, stmtToExec)
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
              SimpleStatement duplicate =
                  SimpleStatement.newInstance(boundStatement.getQuery())
                      .setExecutionProfileName(profile)
                      .setIdempotent(true)
                      .setSerialConsistencyLevel(
                          operationsConfig.queriesConfig().consistency().schemaChanges());

              return Uni.createFrom()
                  .item(throwable)
                  .onItem()
                  .delayIt()
                  .by(Duration.ofMillis(operationsConfig.databaseConfig().ddlRetryDelayMillis()))
                  .onItem()
                  .transformToUni(v -> executeAsync(requestContext, duplicate));
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

  public Uni<Metadata> getDriverMetadata(RequestContext requestContext) {

    return cqlSessionCache
        .getSession(requestContext)
        .flatMap(session -> Uni.createFrom().completionStage(session::refreshSchemaAsync));
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace
   * @param collectionName
   * @return
   */
  public Uni<Optional<TableMetadata>> getTableMetadata(
      RequestContext requestContext, String namespace, String collectionName) {

    return getDriverMetadata(requestContext)
        .map(
            metadata -> {
              var keyspaceMetadata =
                  metadata.getKeyspaces().get(cqlIdentifierFromUserInput(namespace));

              if (keyspaceMetadata == null) {
                throw SchemaException.Code.UNKNOWN_KEYSPACE.get(
                    ErrorObjectV2Constants.TemplateVars.KEYSPACE, namespace);
              }

              return keyspaceMetadata.getTable(cqlIdentifierFromUserInput(collectionName));
            });
  }

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }

  public CQLSessionCache getCqlSessionCache() {
    return this.cqlSessionCache;
  }
}
