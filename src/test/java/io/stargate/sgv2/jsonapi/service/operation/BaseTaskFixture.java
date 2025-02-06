package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.DriverExceptionHandlerAssertions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;

public class BaseTaskFixture<
    TaskT extends BaseTask<SchemaT, ResultSupplierT, ResultT>,
    SchemaT extends SchemaObject,
    ResultSupplierT extends BaseTask.UniSupplier<ResultT>,
    ResultT> {

  private final OperationAttemptAssertions<BaseTaskFixture<SubT, SchemaT>, SubT, SchemaT>
      attempt;

  private final CommandQueryExecutorAssertions<BaseTaskFixture<SubT, SchemaT>>
      queryExecutor;

  private final DriverExceptionHandlerAssertions<BaseTaskFixture<SubT, SchemaT>, SchemaT>
      exceptionHandler;

  private final AsyncResultSet resultSet;

  public BaseTaskFixture(
      BaseTask<SubT, SchemaT, ?> attempt,
      CommandQueryExecutor queryExecutor,
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory,
      AsyncResultSet resultSet) {

    this.exceptionHandler = new DriverExceptionHandlerAssertions<>(this, exceptionHandlerFactory);
    // the assertions for the exceptionHandlerFactory wrap the original factory so we know when it
    // is
    // called and can run assertions on the handler
    this.attempt =
        new OperationAttemptAssertions<>(
            this, attempt, queryExecutor, this.exceptionHandler.getHandlerFactory());
    this.queryExecutor = new CommandQueryExecutorAssertions<>(this, queryExecutor);
    this.resultSet = resultSet;
  }

  public OperationAttemptAssertions<BaseTaskFixture<SubT, SchemaT>, SubT, SchemaT>
      attempt() {
    return attempt;
  }

  public CommandQueryExecutorAssertions<BaseTaskFixture<SubT, SchemaT>> queryExecutor() {
    return queryExecutor;
  }

  public DriverExceptionHandlerAssertions<BaseTaskFixture<SubT, SchemaT>, SchemaT>
      exceptionHandler() {
    return exceptionHandler;
  }

  public AsyncResultSet resultSet() {
    return resultSet;
  }
}
