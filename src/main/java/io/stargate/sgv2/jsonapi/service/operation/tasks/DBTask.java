package io.stargate.sgv2.jsonapi.service.operation.tasks;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttemptPage;
import io.stargate.sgv2.jsonapi.util.CqlPrintUtil;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single query we want to run against the database.
 *
 * <p>This can be any type of DML or DDL statement.
 *
 * <p>Subclasses are responsible for building the query to run, and how to handle the results. This
 * super class knows how to be a task that is making a call to the Cassandra java driver.
 *
 * @param <SchemaT> The type of the schema object the task is working with.
 */
public abstract class DBTask<SchemaT extends SchemaObject>
    extends BaseTask<SchemaT, DBTask.AsyncResultSetSupplier, AsyncResultSet> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBTask.class);

  protected final DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory;

  /**
   * Create a new {@link DBTask} with the provided position, schema object, retry policy and
   * exception handler factory.
   *
   * @param position See {@link BaseTask#BaseTask(int, SchemaObject, TaskRetryPolicy)}
   * @param schemaObject See {@link BaseTask#BaseTask(int, SchemaObject, TaskRetryPolicy)}
   * @param retryPolicy See {@link BaseTask#BaseTask(int, SchemaObject, TaskRetryPolicy)}
   * @param exceptionHandlerFactory Factory to create a new {@link DriverExceptionHandler} that is
   *     called when an exception is thrown after the driver retry policy.
   */
  protected DBTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    super(position, schemaObject, retryPolicy);

    this.exceptionHandlerFactory =
        Objects.requireNonNull(exceptionHandlerFactory, "exceptionHandlerFactory cannot be null");
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  /**
   * A {@link UniSupplier} that for fetching a {@link AsyncResultSet}.
   *
   * <p>Includes the {@link SimpleStatement} so error mapping can include the CQL if needed. Note
   * that the statement can change from request to request, e.g. if the first request missed an
   * ALLOW FILTERING the next may have it.
   */
  public static class AsyncResultSetSupplier implements BaseTask.UniSupplier<AsyncResultSet> {
    protected final SimpleStatement statement;
    protected final BaseTask.UniSupplier<AsyncResultSet> supplier;

    /**
     * Create a new {@link AsyncResultSetSupplier} with the provided statement and supplier.
     *
     * @param statement The statement is being executed, may be null if the supplier is not using a
     *     statement. e.g. some Metadata tasks done need one.
     * @param supplier The supplier that will provide the {@link AsyncResultSet} when called.
     */
    public AsyncResultSetSupplier(
        SimpleStatement statement, BaseTask.UniSupplier<AsyncResultSet> supplier) {
      this.statement = statement;
      this.supplier = Objects.requireNonNull(supplier, "supplier must not be null");
    }

    @Override
    public Uni<AsyncResultSet> get() {
      return supplier.get();
    }
  }

  /** {@inheritDoc} */
  @Override
  protected AsyncResultSetSupplier buildResultSupplier(CommandContext<SchemaT> commandContext) {

    // TODO: HACK: aaron feb 4th '25, quick code to get the command query executor we use with tasks , improve later
    var commandQueryExecutor =
        new CommandQueryExecutor(
            commandContext.cqlSessionCache(),
            new CommandQueryExecutor.DBRequestContext(
                commandContext.requestContext().getTenantId(),
                commandContext.requestContext().getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    return buildResultSupplier(commandQueryExecutor);
  }

  /** {@inheritDoc} */
  @Override
  protected RuntimeException maybeHandleException(AsyncResultSetSupplier resultSupplier, RuntimeException runtimeException) {
    return exceptionHandlerFactory
        .apply(schemaObject, resultSupplier.statement)
        .maybeHandle(runtimeException);
  }

  // =================================================================================================
  // Subclass API
  // =================================================================================================

  /**
   * Subclasses must implement this method to build the query and provide a supplier that executes
   * query and returns results. They should not do anything with Uni for retry etc..
   *
   * @param queryExecutor The {@link CommandQueryExecutor} for subclasses to access the database
   *     with.
   * @return A {@link AsyncResultSetSupplier} that has the statement (if any) and supplier to get the
   *     <code>Uni<AsyncResultSet></code> when it is called. It is important that the DB call not
   *     happen until the supplier is called, otherwise this will block.
   */
  protected abstract AsyncResultSetSupplier buildResultSupplier(CommandQueryExecutor queryExecutor);

  /**
   * Called to get the description of the schema to use when building the response.
   *
   * @return The optional object that describes the schema, if present the object will be serialised
   *     to JSON and included in the response status such as {@link
   *     CommandStatus#PRIMARY_KEY_SCHEMA}. How this is included in the response is up to the {@link
   *     TaskPage} that is building the response.
   */
  public Optional<ColumnsDescContainer> schemaDescription() {
    return Optional.empty();
  }

  // =================================================================================================
  // Internal and helpers
  // =================================================================================================

  /**
   * Helper to log CQL statements at debug and trace level, trimming vectors and other long values.
   */
  protected void logStatement(Logger logger, String prefix, SimpleStatement statement) {
    //  vectors are very big, so we do not log them at debug they will blow up the logs
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} - {}, cql={}, values={}",
          prefix,
          positionAndTaskId(),
          CqlPrintUtil.trimmedCql(statement),
          CqlPrintUtil.trimmedPositionalValues(statement));
    }

    if (logger.isTraceEnabled()) {
      logger.trace(
          "{} - {}, cql={}, values={}",
          prefix,
          positionAndTaskId(),
          statement.getQuery(),
          statement.getPositionalValues());
    }
  }
}
