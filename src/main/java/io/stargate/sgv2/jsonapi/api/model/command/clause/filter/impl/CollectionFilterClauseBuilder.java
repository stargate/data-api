package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;

public class CollectionFilterClauseBuilder extends FilterClauseBuilder<CollectionSchemaObject> {
  public CollectionFilterClauseBuilder(CollectionSchemaObject schema) {
    super(schema);
  }

  @Override
  protected FilterClause build(LogicalExpression implicitAnd) {
    return new CollectionFilterClause(implicitAnd);
  }
}
