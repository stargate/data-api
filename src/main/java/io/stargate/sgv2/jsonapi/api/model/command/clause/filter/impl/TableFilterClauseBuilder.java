package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

public class TableFilterClauseBuilder extends FilterClauseBuilder<TableSchemaObject> {
  public TableFilterClauseBuilder(TableSchemaObject schema) {
    super(schema);
  }

  // Tables do not have fixed "_id" as THE document id
  @Override
  protected boolean isDocId(String path) {
    return false;
  }

  @Override
  protected FilterClause buildAndValidate(LogicalExpression implicitAnd) {
    return new TableFilterClause(implicitAnd).validate(schema);
  }
}
