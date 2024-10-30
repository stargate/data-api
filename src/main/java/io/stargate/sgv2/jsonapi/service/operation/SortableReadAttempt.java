package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.ResultRowContainer;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.CqlOptions;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.SelectCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This class implements paginated read attempts with in memory sorting.
 * Number of records to be read limit of `operationsConfig.maxDocumentSortCount() + 1`
 */
public class SortableReadAttempt extends ReadAttempt<TableSchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortableReadAttempt.class);

  private final ResultRowContainer rowContainer;

  public SortableReadAttempt(
      int position,
      TableSchemaObject schemaObject,
      SelectCQLClause selectCQLClause,
      WhereCQLClause<Select> whereCQLClause,
      OrderByCqlClause orderByCqlClause,
      CqlOptions<Select> cqlOptions,
      CqlPagingState pagingState,
      DocumentSourceSupplier documentSourceSupplier,
      ResultRowContainer rowContainer) {
    super(
        position,
        schemaObject,
        selectCQLClause,
        whereCQLClause,
        orderByCqlClause,
        cqlOptions,
        pagingState,
        documentSourceSupplier);
    this.rowContainer = rowContainer;
  }

  protected boolean inMemorySort() {
    return true;
  }

  protected ResultRowContainer inMemoryResultSetContainer() {
    return rowContainer;
  }
}
