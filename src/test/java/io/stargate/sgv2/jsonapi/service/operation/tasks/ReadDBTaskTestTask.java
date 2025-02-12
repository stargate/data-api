package io.stargate.sgv2.jsonapi.service.operation.tasks;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.OperationProjection;
import io.stargate.sgv2.jsonapi.service.operation.ReadDBTask;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;

/**
 * A test the mocks reading from the database, using the {@link ReadDBTask} to read from the
 * database
 */
public class ReadDBTaskTestTask extends ReadDBTask<TableSchemaObject> {

  private final AsyncResultSet resultSet;

  ReadDBTaskTestTask(
      int position,
      TableSchemaObject schemaObject,
      DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      OrderByCqlClause orderByCqlClause,
      CQLOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      OperationProjection projection,
      AsyncResultSet resultSet) {
    super(
        position,
        schemaObject,
        TableDriverExceptionHandler::new,
        selectCQLClause,
        whereCQLClause,
        orderByCqlClause,
        cqlOptions,
        pagingState,
        RowSorter.NO_OP,
        projection);
    this.resultSet = resultSet;
  }
}
