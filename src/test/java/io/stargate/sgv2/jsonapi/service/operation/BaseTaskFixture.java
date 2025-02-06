package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.DriverExceptionHandlerAssertions;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;

public class BaseTaskFixture {

  private final BaseTaskAssertions<BaseTaskFixture<SubT, SchemaT>, SubT, SchemaT>
      task;

  private final CommandQueryExecutorAssertions<BaseTaskFixture<SubT, SchemaT>>
      queryExecutor;

  private final DriverExceptionHandlerAssertions<BaseTaskFixture<SubT, SchemaT>, SchemaT>
      exceptionHandler;

  private final AsyncResultSet resultSet;

  public BaseTaskFixture(
      TestBaseTask task,
      CommandContext<TableSchemaObject> commandContext,
      AsyncResultSet resultSet) {

    this.task =
        new BaseTaskAssertions<>(
            this, attempt, queryExecutor, this.exceptionHandler.getHandlerFactory());
    this.queryExecutor = new CommandQueryExecutorAssertions<>(this, queryExecutor);
    this.resultSet = resultSet;
  }

  public BaseTaskAssertions<BaseTaskFixture<SubT, SchemaT>, SubT, SchemaT>
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
