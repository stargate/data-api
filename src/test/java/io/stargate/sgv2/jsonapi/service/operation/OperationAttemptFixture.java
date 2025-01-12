package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutorAssertions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tables.DriverExceptionHandlerAssertions;

public class OperationAttemptFixture<
    SubT extends OperationAttempt<SubT, SchemaT>, SchemaT extends SchemaObject> {

  private final OperationAttemptAssertions<OperationAttemptFixture<SubT, SchemaT>, SubT, SchemaT>
      attempt;
  private final CommandQueryExecutorAssertions<OperationAttemptFixture<SubT, SchemaT>>
      queryExecutor;
  private final DriverExceptionHandlerAssertions<OperationAttemptFixture<SubT, SchemaT>, SchemaT>
      exceptionHandler;
  private final AsyncResultSet resultSet;

  public OperationAttemptFixture(
      OperationAttempt<SubT, SchemaT> attempt,
      CommandQueryExecutor queryExecutor,
      DriverExceptionHandler exceptionHandler,
      AsyncResultSet resultSet) {
    this.attempt = new OperationAttemptAssertions<>(this, attempt, queryExecutor, exceptionHandler);
    this.queryExecutor = new CommandQueryExecutorAssertions<>(this, queryExecutor);
    this.exceptionHandler = new DriverExceptionHandlerAssertions<>(this, exceptionHandler);
    this.resultSet = resultSet;
  }

  public OperationAttemptAssertions<OperationAttemptFixture<SubT, SchemaT>, SubT, SchemaT>
      attempt() {
    return attempt;
  }

  public CommandQueryExecutorAssertions<OperationAttemptFixture<SubT, SchemaT>> queryExecutor() {
    return queryExecutor;
  }

  public DriverExceptionHandlerAssertions<OperationAttemptFixture<SubT, SchemaT>, SchemaT>
      exceptionHandler() {
    return exceptionHandler;
  }

  public AsyncResultSet resultSet() {
    return resultSet;
  }
}
