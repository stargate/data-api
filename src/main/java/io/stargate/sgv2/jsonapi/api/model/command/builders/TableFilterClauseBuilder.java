package io.stargate.sgv2.jsonapi.api.model.command.builders;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;

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
  protected FilterClause validateAndBuild(LogicalExpression implicitAnd) {
    return new FilterClause(implicitAnd);
  }
}
