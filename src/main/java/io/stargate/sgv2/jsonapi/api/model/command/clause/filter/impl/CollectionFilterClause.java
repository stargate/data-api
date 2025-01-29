package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;

public class CollectionFilterClause extends FilterClause {
  public CollectionFilterClause(LogicalExpression logicalExpression) {
    super(logicalExpression);
  }
}
