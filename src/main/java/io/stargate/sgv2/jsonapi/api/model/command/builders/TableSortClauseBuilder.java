package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtCqlIdentifier;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** {@link SortClauseBuilder} to use with Tables. */
public class TableSortClauseBuilder extends SortClauseBuilder<TableSchemaObject> {
  private record SortExpressionDefinition(String path, JsonNode sortValue, ApiColumnDef column) {}

  public TableSortClauseBuilder(TableSchemaObject table) {
    super(table);
  }

  @Override
  protected SortClause buildClauseFromDefinition(ObjectNode sortNode) {
    // First, resolve the paths to column definitions
    final List<SortExpressionDefinition> sortExprDefs = resolveColumns(sortNode);
    final List<SortExpression> sortExpressions = new ArrayList<>();
    for (SortExpressionDefinition sortExprDef : sortExprDefs) {
      sortExpressions.add(buildSortExpression(sortExprDef.path, sortExprDef.sortValue));
    }
    return new SortClause(sortExpressions);
  }

  private List<SortExpressionDefinition> resolveColumns(ObjectNode sortNode) {
    List<SortExpressionDefinition> sortExprDefs = new ArrayList<>();
    final List<CqlIdentifier> unknownColumnNames = new ArrayList<>();
    final ApiColumnDefContainer columns = schema.apiTableDef().allColumns();

    for (Map.Entry<String, JsonNode> entry : sortNode.properties()) {
      String path = entry.getKey();
      JsonNode innerValue = entry.getValue();

      // Resolve the column for the path
      CqlIdentifier columnName = CqlIdentifierUtil.cqlIdentifierFromUserInput(path);
      ApiColumnDef column = columns.get(columnName);
      if (column == null) {
        unknownColumnNames.add(columnName);
        continue;
      }
      sortExprDefs.add(new SortExpressionDefinition(path, innerValue, column));
    }

    if (!unknownColumnNames.isEmpty()) {
      throw SortException.Code.CANNOT_SORT_UNKNOWN_COLUMNS.get(
          errVars(
              schema,
              map -> {
                map.put("allColumns", errFmtApiColumnDef(columns));
                map.put("unknownColumns", errFmtCqlIdentifier(unknownColumnNames));
              }));
    }
    return sortExprDefs;
  }

  @Override
  protected SortExpression buildSortExpression(String path, JsonNode innerValue) {
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
    return super.buildSortExpression(path, innerValue);
  }
}
