package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSourceSupplier;
import io.stargate.sgv2.jsonapi.service.operation.ReadAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;

public class TableReadAttempt extends ReadAttempt<TableSchemaObject> {

  TableReadAttempt(
      TableSchemaObject tableSchemaObject,
      int position,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      CqlOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      DocumentSourceSupplier documentSourceSupplier) {
    super(
        position,
        tableSchemaObject,
        selectCQLClause,
        whereCQLClause,
        cqlOptions,
        pagingState,
        documentSourceSupplier);

    setStatus(OperationStatus.READY);
  }
}
