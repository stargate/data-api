package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SchemaAttempt<SchemaT extends SchemaObject>
    extends OperationAttempt<SchemaAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaAttempt.class);

  protected SchemaAttempt(int position, SchemaT schemaObject, RetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
  }

  @Override
  protected Uni<AsyncResultSet> execute(CommandQueryExecutor queryExecutor) {
    var statement = buildStatement();

    LOGGER.warn("SCHEMA CQL: {}", statement.getQuery());
    LOGGER.warn("SCHEMA VALUES: {}", statement.getPositionalValues());

    return queryExecutor.executeRead(statement);
  }

  @Override
  protected SchemaAttempt<SchemaT> onSuccess(AsyncResultSet resultSet) {
    return null;
  }

  protected abstract SimpleStatement buildStatement();

  public static class SchemaRetryPolicy extends RetryPolicy {

    public SchemaRetryPolicy(int maxRetries, Duration delay) {
      super(maxRetries, delay);
    }

    @Override
    boolean shouldRetry(Throwable throwable) {
      // AARON - this is copied from QueryExecutor.executeSchemaChange()
      return throwable instanceof DriverTimeoutException
          || throwable instanceof InvalidQueryException
          || (throwable instanceof TruncateException
              && "Failed to interrupt compactions".equals(throwable.getMessage()));
    }
  }
}
