package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtApiColumnDef;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtCqlIdentifier;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtJoin;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDefContainer;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
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
    var sortExprDefs = resolveColumns(sortNode);

    // Then split into "special" (vector/vectorize, lexical) and regular expressions
    var lexicalExprs = new ArrayList<SortExpressionDefinition>();
    var vectorExprs = new ArrayList<SortExpressionDefinition>();
    var regularExprs = new ArrayList<SortExpressionDefinition>();

    for (SortExpressionDefinition sortExprDef : sortExprDefs) {
      var column = sortExprDef.column;
      switch (column.type().typeName()) {
        case VECTOR -> vectorExprs.add(sortExprDef);
        case ASCII, TEXT -> {
          if (sortExprDef.sortValue.isTextual()) {
            lexicalExprs.add(sortExprDef);
          } else {
            regularExprs.add(sortExprDef);
          }
        }
        default -> regularExprs.add(sortExprDef);
      }
    }

    // Lexical(s)? Must have but one expression, cannot be combined with other sorts
    // Ditto for vector/vectorize
    if (!lexicalExprs.isEmpty() || !vectorExprs.isEmpty()) {
      if (sortExprDefs.size() > 1) {
        throw SortException.Code.CANNOT_SORT_ON_SPECIAL_WITH_OTHERS.get(
            errVars(
                schema,
                map -> {
                  map.put("lexicalSorts", columnsDesc(lexicalExprs));
                  map.put("regularSorts", columnsDesc(regularExprs));
                  map.put("vectorSorts", columnsDesc(vectorExprs));
                }));
      }

      if (!lexicalExprs.isEmpty()) {
        return new SortClause(List.of(buildLexicalSortExpression(lexicalExprs.getFirst())));
      }

      return new SortClause(List.of(buildVectorOrVectorizeSortExpression(vectorExprs.getFirst())));
    }

    // Otherwise, we can build regular sort expression(s)
    final List<SortExpression> sortExpressions = new ArrayList<>();
    for (SortExpressionDefinition exprDef : regularExprs) {
      sortExpressions.add(buildRegularSortExpression(exprDef));
    }
    return new SortClause(sortExpressions);
  }

  private List<SortExpressionDefinition> resolveColumns(ObjectNode sortNode) {
    final List<SortExpressionDefinition> sortExprDefs = new ArrayList<>();
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

  protected SortExpression buildLexicalSortExpression(SortExpressionDefinition lexicalExpr) {
    // caller validated JsonNode is textual; for now nothing more to validate
    return SortExpression.tableLexicalSort(lexicalExpr.path(), lexicalExpr.sortValue.textValue());
  }

  protected SortExpression buildVectorOrVectorizeSortExpression(
      SortExpressionDefinition vectorExpr) {
    final String path = vectorExpr.path();
    final JsonNode exprValue = vectorExpr.sortValue();

    // So we know we have a Vector column; now can check if value is a binary vector or an array
    // of floats, or a string to vectorize.
    // For Vectorize, further checks are done in the TableSortClauseResolver.

    // First: vector data either as EJSON binary or as JSON Array (of floats)
    float[] vectorFloats = tryDecodeBinaryVector(path, exprValue);
    if (vectorFloats != null) {
      return SortExpression.tableVectorSort(path, vectorFloats);
    }
    if (exprValue instanceof ArrayNode innerArray) {
      return SortExpression.tableVectorSort(path, arrayNodeToVector(innerArray));
    }

    // Otherwise, check if it is a String to vectorize
    if (exprValue.isTextual()) {
      return SortExpression.tableVectorizeSort(path, exprValue.textValue());
    }

    // Otherwise, invalid (cannot be a regular sort as it is a Vector column)
    throw SortException.Code.INVALID_VECTOR_SORT_EXPRESSION.get(
        errVars(
            schema,
            map -> {
              map.put("jsonType", JsonUtil.nodeTypeAsString(exprValue));
            }));
  }

  /**
   * Helper method to build a "non-special" sort expression for given definition; validates
   * expression value and builds the {@link SortExpression} object.
   */
  private SortExpression buildRegularSortExpression(SortExpressionDefinition exprDef) {
    JsonNode innerValue = exprDef.sortValue();
    if (!innerValue.isInt()) {
      // Special checking for String and ArrayNode to give less confusing error messages
      if (innerValue.isTextual()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
            "Sort ordering value can be String only for Lexical or Vectorize search");
      }
      if (innerValue.isArray()) {
        throw SortException.Code.CANNOT_VECTOR_SORT_NON_VECTOR_COLUMNS.get(
            errVars(
                schema,
                map -> {
                  map.put(
                      "vectorColumns",
                      errFmtApiColumnDef(
                          schema.apiTableDef().allColumns().filterVectorColumnsToList()));
                  map.put("sortColumns", exprDef.path());
                }));
      }
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
          "Sort ordering value should be integer `1` or `-1`; or Array (Vector); or String (Lexical or Vectorize), was: %s",
          JsonUtil.nodeTypeAsString(innerValue));
    }
    if (!(innerValue.intValue() == 1 || innerValue.intValue() == -1)) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
          "Sort ordering value can only be `1` for ascending or `-1` for descending (not `%s`)",
          innerValue);
    }

    boolean ascending = innerValue.intValue() == 1;
    return SortExpression.sort(exprDef.path(), ascending);
  }

  private String columnsDesc(List<SortExpressionDefinition> sortExprDefs) {
    return errFmtJoin(sortExprDefs.stream().map(SortExpressionDefinition::path).toList());
  }
}
