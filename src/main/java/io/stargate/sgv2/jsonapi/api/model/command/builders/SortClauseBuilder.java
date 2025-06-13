package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.util.JsonUtil.arrayNodeToVector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.ArrayList;
import java.util.Collections;
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

  protected SortClause defaultBuildAndValidate(ObjectNode sortNode) {
    // safe to iterate, we know it's an Object
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    int totalFields = sortNode.size();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());

    // $lexical is only allowed alone: handle first
    JsonNode lexicalValue = sortNode.get(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    if (lexicalValue != null) {
      if (sortNode.size() > 1) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
            "if sorting by '%s' no other sort expressions allowed",
            DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
      }
      if (!lexicalValue.isTextual()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
            "if sorting by '%s' value must be String, not %s",
            DocumentConstants.Fields.LEXICAL_CONTENT_FIELD,
            JsonUtil.nodeTypeAsString(lexicalValue));
      }
      // We cannot yet determine if lexical sort supported by the collection, just
      // construct clause
      return new SortClause(
          Collections.singletonList(SortExpression.bm25Search(lexicalValue.textValue())));
    }

    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      final String path = inner.getKey().trim();
      // Validation will check against invalid paths, as well as decode "amp-escaping"
      final String validatedPath = validateSortClausePath(path);

      float[] vectorFloats = null;
      if (inner.getValue().isObject()) {
        var ejsonWrapped = EJSONWrapper.maybeFrom((ObjectNode) inner.getValue());
        if (ejsonWrapped == null) {
          throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
              "Only binary vector object values is supported for sorting. Path: %s, Value: %s.",
              path, inner.getValue().toString());
        }
        try {
          vectorFloats = ejsonWrapped.getVectorValueForBinary();
        } catch (IllegalArgumentException | IllegalStateException e) {
          throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(e.getMessage());
        }
      }
      // handle table vector sort
      if (!(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)
          || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path))) {
        if (vectorFloats != null) {
          sortExpressions.add(SortExpression.tableVectorSort(path, vectorFloats));
          continue;
        } else if (inner.getValue().isArray()) {
          // TODO: HACK: quick support for tables, if the value is an array we will assume the
          // column
          // is a vector then need to check on table pathway that the sort is correct.
          // NOTE: does not check if there are more than one sort expression, the
          // TableSortClauseResolver will take care of that so we can get proper ApiExceptions
          // this is also why we do not break the loop here
          sortExpressions.add(
              SortExpression.tableVectorSort(
                  path, arrayNodeToVector((ArrayNode) inner.getValue())));
          continue;
        }
      }
      if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
        // Vector search can't be used with other sort clause
        if (totalFields > 1) {
          throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
        }
        if (vectorFloats == null) {
          if (!inner.getValue().isArray()) {
            throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
          } else {
            ArrayNode arrayNode = (ArrayNode) inner.getValue();
            vectorFloats = new float[arrayNode.size()];
            if (arrayNode.isEmpty()) {
              throw ErrorCodeV1.SHRED_BAD_VECTOR_SIZE.toApiException();
            }
            for (int i = 0; i < arrayNode.size(); i++) {
              JsonNode element = arrayNode.get(i);
              if (!element.isNumber()) {
                throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
              }
              vectorFloats[i] = element.floatValue();
            }
          }
        }
        SortExpression exp = SortExpression.vsearch(vectorFloats);
        sortExpressions.clear();
        sortExpressions.add(exp);
        // TODO: aaron 17-oct-2024 - this break seems unneeded as above it checks if there is only
        // 1
        // field, leaving for now
        break;

      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)) {
        // Vector search can't be used with other sort clause
        if (totalFields > 1) {
          throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
        }
        if (!inner.getValue().isTextual()) {
          throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
        }

        String vectorizeData = inner.getValue().textValue();
        if (vectorizeData.isBlank()) {
          throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
        }
        SortExpression exp = SortExpression.vectorizeSearch(vectorizeData);
        sortExpressions.clear();
        sortExpressions.add(exp);
        // TODO: aaron 17-oct-2024 - this break seems unneeded as above it checks if there is only 1
        // field, leaving for now
        break;
      } else if (inner.getValue().isArray()) {
        // TODO: HACK: quick support for tables, if the value is an array we will assume the column
        // is a vector then need to check on table pathway that the sort is correct.
        // NOTE: does not check if there are more than one sort expression, the
        // TableCqlSortClauseResolver will take care of that so we can get proper ApiExceptions
        // this is also why we do not break the look here
        sortExpressions.add(
            SortExpression.tableVectorSort(path, arrayNodeToVector((ArrayNode) inner.getValue())));
      } else if (inner.getValue().isTextual()) {
        // TODO: HACK: quick support for tables, if the value is an text  we will assume the column
        // is a vector and the user wants to do vectorize then need to check on table pathway that
        // the sort is correct.
        // NOTE: does not check if there are more than one sort expression, the
        // TableSortClauseResolver will take care of that so we can get proper ApiExceptions
        // this is also why we do not break the look here
        sortExpressions.add(SortExpression.tableVectorizeSort(path, inner.getValue().textValue()));
      } else {
        if (!inner.getValue().isInt()
            || !(inner.getValue().intValue() == 1 || inner.getValue().intValue() == -1)) {
          throw ErrorCodeV1.INVALID_SORT_CLAUSE_VALUE.toApiException(
              "Sort ordering value can only be `1` for ascending or `-1` for descending (not `%s`)",
              inner.getValue());
        }

        boolean ascending = inner.getValue().intValue() == 1;
        SortExpression exp = SortExpression.sort(validatedPath, ascending);
        sortExpressions.add(exp);
      }
    }
    return new SortClause(sortExpressions);
  }

  private String validateSortClausePath(String path) {
    if (!NamingRules.FIELD.apply(path)) {
      if (path.isEmpty()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
            "path must be represented as a non-empty string");
      }
      // But allow "well-known" fields
      switch (path) {
        case DocumentConstants.Fields.LEXICAL_CONTENT_FIELD,
            DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
            DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD -> {}
        default ->
            throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
                "path ('%s') cannot start with '$' (except for pseudo-fields '$lexical', '$vector' and '$vectorize')",
                path);
      }
    }

    try {
      path = DocumentPath.verifyEncodedPath(path);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "sort clause path ('%s') is not a valid path. " + e.getMessage(), path);
    }

    return path;
  }
}
