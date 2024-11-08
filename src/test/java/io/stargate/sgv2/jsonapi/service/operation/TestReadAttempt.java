package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.*;

public class TestReadAttempt extends ReadAttempt<TableSchemaObject> {

  private final AsyncResultSet resultSet;

  TestReadAttempt(
      int position,
      TableSchemaObject schemaObject,
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
