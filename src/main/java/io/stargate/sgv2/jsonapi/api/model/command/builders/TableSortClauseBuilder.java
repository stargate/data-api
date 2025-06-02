package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** {@link SortClauseBuilder} to use with Tables. */
public class TableSortClauseBuilder extends SortClauseBuilder<TableSchemaObject> {
  public TableSortClauseBuilder(TableSchemaObject table) {
    super(table);
  }

  @Override
  public SortClause buildAndValidate(ObjectNode sortNode) {
    return defaultBuildAndValidate(sortNode);
  }
}
