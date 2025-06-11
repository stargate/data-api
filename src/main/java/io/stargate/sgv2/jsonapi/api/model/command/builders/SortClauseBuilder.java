package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Object for converting {@link JsonNode} (from {@link SortDefinition}) into {@link SortClause}.
 * Process will validate structure of the JSON, and also validate values of the sort expressions.
 */
public abstract class SortClauseBuilder<T extends SchemaObject> {
  protected final T schema;

  protected SortClauseBuilder(T schema) {
    this.schema = Objects.requireNonNull(schema);
  }

  public static SortClauseBuilder<?> builderFor(SchemaObject schema) {
    return switch (schema) {
      case CollectionSchemaObject collection -> new CollectionSortClauseBuilder(collection);
      case TableSchemaObject table -> new TableSortClauseBuilder(table);
      default ->
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported schema object class for `SortClauseBuilder`: %s",
                  schema.getClass()));
    };
  }

  public SortClause build(JsonNode node) {
    // if missing or null, return "empty" sort clause
    if (node.isMissingNode() || node.isNull()) {
      return SortClause.empty();
    }

    // otherwise, if it's not object throw exception
    if (!(node instanceof ObjectNode sortNode)) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
          "Sort clause must be submitted as json object");
    }

    return buildAndValidate(sortNode);
  }

  protected abstract SortClause buildAndValidate(ObjectNode sortNode);

  protected abstract String validateSortClausePath(String path);

  protected SortClause defaultBuildAndValidate(ObjectNode sortNode) {
    // safe to iterate, we know it's an Object
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    int totalFields = sortNode.size();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());

    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      final String path = inner.getKey().trim();
      // Validation will check against invalid paths, as well as decode "amp-escaping"
      sortExpressions.add(
          buildAndValidateExpression(
              path, validateSortClausePath(path), inner.getValue(), totalFields));
    }
    return new SortClause(sortExpressions);
  }

  protected abstract SortExpression buildAndValidateExpression(
      String path, String validatedPath, JsonNode innerValue, int totalFields);

  protected SortExpression defaultBuildAndValidateExpression(
      String path, String validatedPath, JsonNode innerValue) {
    if (!innerValue.isInt()) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
          "Sort ordering value should be integer `1` or `-1`; or Array of numbers (Vector); or String but was: %s",
          JsonUtil.nodeTypeAsString(innerValue));
    }
    if (!(innerValue.intValue() == 1 || innerValue.intValue() == -1)) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
          "Sort ordering value can only be `1` for ascending or `-1` for descending (not `%s`)",
          innerValue);
    }

    boolean ascending = innerValue.intValue() == 1;
    return SortExpression.sort(validatedPath, ascending);
  }

  protected float[] tryDecodeBinaryVector(String path, JsonNode innerValue) {
    if (innerValue instanceof ObjectNode innerObject) {
      var ejsonWrapped = EJSONWrapper.maybeFrom(innerObject);
      if (ejsonWrapped == null || ejsonWrapped.type() != EJSONWrapper.EJSONType.BINARY) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
            "Only binary vector object values is supported for sorting. Path: %s, Value: %s.",
            path, innerValue.toString());
      }
      try {
        return ejsonWrapped.getVectorValueForBinary();
      } catch (IllegalArgumentException | IllegalStateException e) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(e.getMessage());
      }
    }
    return null;
  }
}
