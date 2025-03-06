package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.AccumulatingAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configured to execute queries for a specific command that relies on drive profiles MORE TODO
 * WORDS
 *
 * <p><b>NOTE:</b> this is a WIP replacing the earlier QueryExecutor that was built with injection.
 * This is for use by a {@link io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask}
 *
 * <p>The following settings should be set via the driver profile:
 *
 * <ul>
 *   <li><code>page-size</code>
 *   <li><code>consistency</code>
 *   <li><code>serial-consistency</code>
 *   <li><code>default-idempotence</code>
 * </ul>
 */
public class CommandQueryExecutor {

  private static final String PROFILE_NAME_SEPARATOR = "-";

  public enum QueryTarget {
    TABLE,
    COLLECTION;

    final String profilePrefix;

    QueryTarget() {
      this.profilePrefix = name().toLowerCase();
    }
  }

  private enum QueryType {
    CREATE_SCHEMA,
    READ,
    TRUNCATE,
    WRITE;

    final String profileSuffix;

    QueryType() {
      this.profileSuffix = name().toLowerCase();
    }
  }

  private final CQLSessionCache cqlSessionCache;
  private final DBRequestContext dbRequestContext;
  private final QueryTarget queryTarget;

  public CommandQueryExecutor(
      CQLSessionCache cqlSessionCache, DBRequestContext dbRequestContext, QueryTarget queryTarget) {
    this.cqlSessionCache =
        Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
    this.dbRequestContext =
        Objects.requireNonNull(dbRequestContext, "dbRequestContext must not be null");
    this.queryTarget = queryTarget;
  }

  public Uni<AsyncResultSet> executeRead(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.READ);
    return executeAndWrap(statement);
  }

  /**
   * Query executor to read all the pages that satisfy the query, it will keep fetching pages until
   * there are no more.
   *
   * <p>It needs to do this without blocking on the reactive thread.
   *
   * @param statement The statement to execute.
   * @param rowAccumulator The accumulator to hold the rows that are read as we go through the
   *     pages. The accumulator can keep them all or discard some / all as they are read.
   */
  public Uni<AsyncResultSet> executeReadAllPages(
      SimpleStatement statement, RowAccumulator rowAccumulator) {
    Objects.requireNonNull(statement, "statement must not be null");

    var accumulator = new AccumulatingAsyncResultSet(rowAccumulator);

    return Multi.createBy()
        .repeating()
        .uni(
            () -> new AtomicReference<AsyncResultSet>(null), // the state passed to the producer
            stateRef -> {
              Uni<AsyncResultSet> result =
                  stateRef.get() == null
                      ? executeRead(statement) // AsyncResultSet is null so first page
                      : Uni.createFrom()
                          .completionStage(stateRef.get().fetchNextPage()); // After first page
              // returning result for looping, this is remembering the rows as we page through them
              return result
                  .onItem()
                  .invoke(
                      rs -> {
                        accumulator.accumulate(rs);
                        stateRef.set(rs);
                      });
            })
        // Documents read until pageState available, max records read is deleteLimit + 1 TODO
        // COMMENTS
        .whilst(AsyncPagingIterable::hasMorePages)
        .collect()
        .asList()
        .onItem()
        .transformToUni(resultSets -> Uni.createFrom().item(accumulator));
  }

  public Uni<AsyncResultSet> executeWrite(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.WRITE);
    return executeAndWrap(statement);
  }

  public Uni<AsyncResultSet> executeTruncate(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.TRUNCATE);
    return executeAndWrap(statement);
  }

  /**
   * Get the metadata for the given keyspace using session.
   *
   * @param keyspace The keyspace name.
   * @return The keyspace metadata if it exists.
   */
  public Optional<KeyspaceMetadata> getKeyspaceMetadata(String keyspace) {
    return session()
        .getMetadata()
        .getKeyspace(CqlIdentifierUtil.cqlIdentifierFromUserInput(keyspace));
  }

  public Uni<AsyncResultSet> executeCreateSchema(SimpleStatement statement) {
    Objects.requireNonNull(statement, "statement must not be null");

    statement = withExecutionProfile(statement, QueryType.CREATE_SCHEMA);
    return executeAndWrap(statement);
  }

  private CqlSession session() {
    return cqlSessionCache.getSession(
        dbRequestContext.tenantId().orElse(""), dbRequestContext.authToken().orElse(""));
  }

  private String getExecutionProfile(QueryType queryType) {
    return queryTarget.profilePrefix + PROFILE_NAME_SEPARATOR + queryType.profileSuffix;
  }

  private SimpleStatement withExecutionProfile(SimpleStatement statement, QueryType queryType) {
    return statement.setExecutionProfileName(getExecutionProfile(queryType));
  }

  @VisibleForTesting
  public Uni<AsyncResultSet> executeAndWrap(SimpleStatement statement) {

    // changing tracing creates a new object, avoid if not needed
    var execStatement = dbRequestContext.tracingEnabled() != statement.isTracing() ? statement.setTracing(dbRequestContext.tracingEnabled()) : statement;
    return Uni.createFrom().completionStage(session().executeAsync(execStatement));
  }

  // Aaron - Feb 3 - temp rename while factoring full RequestContext
  public record DBRequestContext(Optional<String> tenantId, Optional<String> authToken, boolean tracingEnabled) {}
}
