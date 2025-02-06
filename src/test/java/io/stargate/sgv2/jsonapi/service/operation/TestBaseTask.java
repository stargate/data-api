package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;

public class TestBaseTask
    extends BaseTask<TableSchemaObject, BaseTask.UniSupplier<AsyncResultSet>, AsyncResultSet> {

  private final AsyncResultSet resultSet;

  TestBaseTask(
      int position,
      TableSchemaObject schemaObject,
      TaskRetryPolicy retryPolicy,
      AsyncResultSet resultSet) {
    super(position, schemaObject, retryPolicy);
    this.resultSet = resultSet;
  }


  @Override
  protected UniSupplier<AsyncResultSet> buildResultSupplier(CommandContext<TableSchemaObject> commandContext) {
    return new DBTask.AsyncResultSetSupplier(null, () -> Uni.createFrom().item(this.resultSet));
  }

  @Override
  protected RuntimeException maybeHandleException(UniSupplier<AsyncResultSet> resultSupplier, RuntimeException runtimeException) {
    throw new RuntimeException("Not implemented");
  }
}
