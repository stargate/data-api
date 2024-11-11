package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class TestOperationAttempt
    extends OperationAttempt<TestOperationAttempt, TableSchemaObject> {

  private final AsyncResultSet resultSet;

  TestOperationAttempt(
      int position,
      TableSchemaObject schemaObject,
      RetryPolicy retryPolicy,
      AsyncResultSet resultSet) {
    super(position, schemaObject, retryPolicy);
    this.resultSet = resultSet;
  }

  @Override
  protected Uni<AsyncResultSet> executeStatement(CommandQueryExecutor queryExecutor) {
    return Uni.createFrom().item(this.resultSet);
  }
}
