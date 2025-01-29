package io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl;

import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;

public class CollectionFilterClauseBuilder extends FilterClauseBuilder<CollectionSchemaObject> {
  public CollectionFilterClauseBuilder(CollectionSchemaObject schema) {
    super(schema);
  }

  // Collections have fixed "_id" as THE document id
  @Override
  protected boolean isDocId(String path) {
    return DocumentConstants.Fields.DOC_ID.equals(path);
  }

  @Override
  protected FilterClause buildAndValidate(LogicalExpression implicitAnd) {
    return new CollectionFilterClause(implicitAnd).validate(schema);
  }
}
