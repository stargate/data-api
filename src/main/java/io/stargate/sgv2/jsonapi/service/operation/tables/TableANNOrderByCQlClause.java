package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;

public class TableANNOrderByCQlClause implements OrderByCqlClause {

  private final TableSchemaObject tableSchemaObject;
  private final CqlIdentifier column;
  private final CqlVector<Float> vector;

  public TableANNOrderByCQlClause(TableSchemaObject tableSchemaObject, CqlIdentifier column, CqlVector<Float> vector) {
    this.tableSchemaObject = tableSchemaObject;
    this.column = column;
    this.vector = vector;
  }

  @Override
  public Select apply(Select select) {

    return select.orderByAnnOf(column, vector);
  }
}
