package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.RowsContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;

public class TestReadAttempt extends ReadAttempt<TableSchemaObject> {

  private final AsyncResultSet resultSet;

  TestReadAttempt(
      int position,
      TableSchemaObject schemaObject,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      OrderByCqlClause orderByCqlClause,
      CqlOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      DocumentSourceSupplier documentSourceSupplier,
      AsyncResultSet resultSet) {
    super(
        position,
        schemaObject,
        selectCQLClause,
        whereCQLClause,
        orderByCqlClause,
        cqlOptions,
        pagingState,
        documentSourceSupplier,
        RowsContainer.defaultRowsContainer());
    this.resultSet = resultSet;
  }
}
