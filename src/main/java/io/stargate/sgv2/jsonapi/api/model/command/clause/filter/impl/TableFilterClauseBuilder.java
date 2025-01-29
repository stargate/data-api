package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class TableFilterClauseBuilder extends FilterClauseBuilder<TableSchemaObject> {
  public TableFilterClauseBuilder(TableSchemaObject schema) {
    super(schema);
  }

  @Override
  protected FilterClause build(LogicalExpression implicitAnd) {
    return new TableFilterClause(implicitAnd);
  }
}
