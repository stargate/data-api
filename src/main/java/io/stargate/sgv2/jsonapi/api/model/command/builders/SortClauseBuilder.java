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
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
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

    // First validate the paths for the sort expressions
    validateSortClausePaths(sortNode);

    // And then vui
    return buildAndValidate(sortNode);
  }

  protected void validateSortClausePaths(ObjectNode sortNode) {
    Iterator<String> it = sortNode.fieldNames();
    while (it.hasNext()) {
      validateSortClausePath(it.next());
    }
  }

  protected void validateSortClausePath(String path) {
    // Common checks: blank, bad escaping
    if (path.isBlank()) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "path must be represented as a non-empty string");
    }
    try {
      DocumentPath.verifyEncodedPath(path);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "sort clause path ('%s') is not a valid path. " + e.getMessage(), path);
    }
  }

  protected SortClause buildAndValidate(ObjectNode sortNode) {
    // safe to iterate, we know it's an Object
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    int totalFields = sortNode.size();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());

    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      sortExpressions.add(buildSortExpression(inner.getKey(), inner.getValue(), totalFields));
    }
    return new SortClause(sortExpressions);
  }

  /**
   * Helper method to build a sort expression for given definition. Base implementation is for
   * regular sorts (not vector, vectorize or lexical).
   *
   * @param path Path to the field to sort by, already validated
   * @param innerValue JSON value of the sort expression to use
   * @param sortExpressionCount Total number of sort expressions in the clause
   * @return {@link SortExpression} for the regular sort
   */
  protected SortExpression buildSortExpression(
      String path, JsonNode innerValue, int sortExpressionCount) {
    if (!innerValue.isInt()) {
      // Special checking for String and ArrayNode to give less confusing error messages
      if (innerValue.isArray()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
            "Sort ordering value can be Array only for Vector search");
      }
      if (innerValue.isTextual()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
            "Sort ordering value can be String only for Lexical or Vectorize search");
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
    return SortExpression.sort(path, ascending);
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
