package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class TableFilterClause extends FilterClause {
  public TableFilterClause(LogicalExpression logicalExpression) {
    super(logicalExpression);
  }

  public TableFilterClause validate(TableSchemaObject table) {
    // Nothing to validate yet
    return this;
  }
}
