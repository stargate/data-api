package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task to make a DB schema change.
 *
 * @param <SchemaT>
 */
public abstract class SchemaDBTask<SchemaT extends SchemaObject> extends DBTask<SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaDBTask.class);

  protected SchemaDBTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    super(position, schemaObject, retryPolicy, exceptionHandlerFactory);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildDBResultSupplier(
      CommandContext<SchemaT> commandContext, CommandQueryExecutor queryExecutor) {

    var statement = buildStatement();

    logStatement(LOGGER, "buildResultSupplier()", statement);
    return new AsyncResultSetSupplier(
        commandContext, this, statement, () -> queryExecutor.executeCreateSchema(statement));
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  protected abstract SimpleStatement buildStatement();

  public static class SchemaRetryPolicy extends TaskRetryPolicy {

    public SchemaRetryPolicy(int maxRetries, Duration delay) {
      super(maxRetries, delay);
    }

    @Override
    public boolean shouldRetry(Throwable throwable) {
      // AARON - this is copied from QueryExecutor.executeSchemaChange()
      return throwable instanceof DriverTimeoutException
          || (throwable instanceof QueryExecutionException
              && !(throwable instanceof InvalidQueryException))
          || (throwable instanceof TruncateException
              && "Failed to interrupt compactions".equals(throwable.getMessage()));
    }
  }
}
