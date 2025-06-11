package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
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

  @Override
  protected SortExpression buildAndValidateExpression(
      String path, String validatedPath, JsonNode innerValue, int totalFields) {
    float[] vectorFloats = tryDecodeBinaryVector(path, innerValue);

    // handle table vector sort
    if (vectorFloats != null) {
      return SortExpression.tableVectorSort(path, vectorFloats);
    }
    if (innerValue instanceof ArrayNode innerArray) {
      // TODO: HACK: quick support for tables, if the value is an array we will assume the
      // column is a vector then need to check on table pathway that the sort is correct.
      // NOTE: does not check if there are more than one sort expression, the
      // TableSortClauseResolver will take care of that so we can get proper ApiExceptions
      return SortExpression.tableVectorSort(path, arrayNodeToVector(innerArray));
    }
    if (innerValue.isTextual()) {
      // TODO: HACK: quick support for tables, if the value is an text  we will assume the column
      // is a vector and the user wants to do vectorize then need to check on table pathway that
      // the sort is correct.
      // NOTE: does not check if there are more than one sort expression, the
      // TableSortClauseResolver will take care of that so we can get proper ApiExceptions
      // this is also why we do not break the look here
      return SortExpression.tableVectorizeSort(path, innerValue.textValue());
    }
    return defaultBuildAndValidateExpression(path, validatedPath, innerValue);
  }
}
