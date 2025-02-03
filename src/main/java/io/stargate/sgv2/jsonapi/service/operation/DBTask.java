package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlPrintUtil;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single query we want to run against the database.
 *
 * <p>This can be any type of DML or DDL statement, the operation is responsible for executing the
 * query and handling any errors and retries. Used together with the {@link
 * OperationAttemptContainer}, {@link OperationAttemptPageBuilder} and {@link GenericOperation}
 *
 * <p>Subclasses are responsible for sorting out what query to run, and how to handle the results.
 * This superclass knows how to run a generic query with config such as retries.
 *
 * <p>The class has a basic state model for tracking the status of the operation. <b>NOTE</b>
 * subclasses much set the state of {@link TaskStatus#READY} using {@link
 * #setStatus(TaskStatus)}, other transitions are handled by the superclass.All handling of the
 * state must be done through the methods, do not access the state directly. <b>NOTE:</b> This class
 * is not thread safe, it is used in the Smallrye processing and is not expected to be used in a
 * multithreaded environment.
 *
 * @param <SubT> Subtype of the OperationAttempt, used for chaining methods.
 * @param <SchemaT> The type of the schema object that the operation is working with.
 */
public abstract class DBTask<SchemaT extends SchemaObject>
    extends BaseTask<SchemaT, DBTask.AsyncResultSetSupplier,  AsyncResultSet>
    {

  private static final Logger LOGGER = LoggerFactory.getLogger(DBTask.class);

  protected final DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory;

  /**
   * Create a new {@link DBTask} with the provided position, schema object and retry policy.
   *
   * @param position The 0 based position of the attempt in the container of attempts. Attempts are
   *     ordered by position, for sequential processing and for rebuilding the response in the
   *     correct order (e.g. for inserting many documents)
   * @param schemaObject The schema object that the operation is working with.
   * @param retryPolicy The {@link RetryPolicy} to use when running the operation, if there is no
   *     retry policy then use {@link RetryPolicy#NO_RETRY}
   */
  protected DBTask(int position, SchemaT schemaObject,
                   RetryPolicy retryPolicy,
                   DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    super(position, schemaObject, retryPolicy);

    this.exceptionHandlerFactory = Objects.requireNonNull(exceptionHandlerFactory, "exceptionHandlerFactory cannot be null");
  }

  @Override
  protected RuntimeException maybeHandleException(AsyncResultSetSupplier resultSupplier, RuntimeException runtimeException) {
    return exceptionHandlerFactory.apply(schemaObject, resultSupplier.statement).maybeHandle(runtimeException);
  }


  @Override
  protected AsyncResultSetSupplier buildResultSupplier(CommandContext<SchemaT> commandContext) {

    var commandQueryExecutor = new CommandQueryExecutor(commandContext.cqlSessionCache(),
        new CommandQueryExecutor.DBRequestContext(
            commandContext.requestContext().getTenantId(), commandContext.requestContext().getCassandraToken()),
        CommandQueryExecutor.QueryTarget.TABLE);
    return buildResultSupplier(commandQueryExecutor);
  }

  public static class AsyncResultSetSupplier implements BaseTask.UniSupplier<AsyncResultSet> {
    protected final SimpleStatement statement;
    protected final BaseTask.UniSupplier<AsyncResultSet> supplier;

    protected AsyncResultSetSupplier(
        SimpleStatement statement, BaseTask.UniSupplier<AsyncResultSet> supplier) {
      this.statement = statement;
      this.supplier = supplier;
    }

    @Override
    public Uni<AsyncResultSet> get() {
      return supplier.get();
    }
  }

  /**
   * Subclasses must implement this method to build the query and provide a supplier that executes
   * query and returns results. They should not do anything with Uni for retry etc., that is handled
   * in the base class {@link #execute(CommandQueryExecutor,
   * DefaultDriverExceptionHandler.Factory)}.
   *
   * @param queryExecutor The {@link CommandQueryExecutor} for subclasses to access the database
   *     with.
   * @return A {@link StatementContext} that has the statement (if any) and supplier to get the
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
   *     OperationAttemptPage} that is building the response.
   */
  public Optional<ColumnsDescContainer> schemaDescription() {
    return Optional.empty();
  }

  protected void logStatement(Logger logger, String prefix, SimpleStatement statement) {
    //  vectors are very big, so we do not log them at debug they will blow up the logs
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{} - {}, cql={}, values={}",
          prefix,
          positionAndAttemptId(),
          CqlPrintUtil.trimmedCql(statement),
          CqlPrintUtil.trimmedPositionalValues(statement));
    }

    if (logger.isTraceEnabled()) {
      logger.trace(
          "{} - {}, cql={}, values={}",
          prefix,
          positionAndAttemptId(),
          statement.getQuery(),
          statement.getPositionalValues());
    }
  }
}
