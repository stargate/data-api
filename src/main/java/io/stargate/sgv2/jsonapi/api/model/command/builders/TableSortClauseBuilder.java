package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;

/** {@link SortClauseBuilder} to use with Tables. */
public class TableSortClauseBuilder extends SortClauseBuilder<TableSchemaObject> {
  public TableSortClauseBuilder(TableSchemaObject table) {
    super(table);
  }

  @Override
  public SortClause buildAndValidate(ObjectNode sortNode) {
    return defaultBuildAndValidate(sortNode);
  }

  @Override
  protected String validateSortClausePath(String path) {
    // Tables have few rules: but cannot be empty
    if (path.isEmpty()) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "path must be represented as a non-empty string");
    }
    // Do we use escaping for Tables? For sake of consistency I think so
    try {
      path = DocumentPath.verifyEncodedPath(path);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "sort clause path ('%s') is not a valid path. " + e.getMessage(), path);
    }

    return path;
  }
}
