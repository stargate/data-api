package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;

public class TableFilterClause extends FilterClause {
  public TableFilterClause(LogicalExpression logicalExpression) {
    super(logicalExpression);
  }
}
