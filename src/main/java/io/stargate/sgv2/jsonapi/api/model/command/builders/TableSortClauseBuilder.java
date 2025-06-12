package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;

/** {@link SortClauseBuilder} to use with Tables. */
public class TableSortClauseBuilder extends SortClauseBuilder<TableSchemaObject> {
  public TableSortClauseBuilder(TableSchemaObject table) {
    super(table);
  }

  @Override
  protected SortExpression buildSortExpression(
      String path, JsonNode innerValue, int sortExpressionCount) {
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
    return super.buildSortExpression(path, innerValue, sortExpressionCount);
  }
}
