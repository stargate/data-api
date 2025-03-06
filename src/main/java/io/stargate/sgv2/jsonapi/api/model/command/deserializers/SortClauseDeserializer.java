package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** {@link StdDeserializer} for the {@link SortClause}. */
public class SortClauseDeserializer extends StdDeserializer<SortClause> {

  /** No-arg constructor explicitly needed. */
  public SortClauseDeserializer() {
    super(SortClause.class);
  }

  /** {@inheritDoc} */
  @Override
  public SortClause deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
    JsonNode node = ctxt.readTree(parser);

    // if missing or null, return null back
    if (node.isMissingNode() || node.isNull()) {
      // TODO should we return empty sort clause instead?
      return null;
    }

    // otherwise, if it's not object throw exception
    if (!node.isObject()) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
          "Sort clause must be submitted as json object");
    }

    ObjectNode sortNode = (ObjectNode) node;

    // safe to iterate, we know it's an Object
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    int totalFields = sortNode.size();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());

    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      final String path = inner.getKey().trim();
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
          // this is also why we do not break the look here
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
        String validatedPath = validateSortClausePath(path);

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

  /**
   * TODO: this almost duplicates code in WriteableShreddedDocument.shredVector() but that does not
   * check the array elements, we MUST stop duplicating code like this
   */
  private static float[] arrayNodeToVector(ArrayNode arrayNode) {

    float[] arrayVals = new float[arrayNode.size()];
    if (arrayNode.isEmpty()) {
      throw ErrorCodeV1.SHRED_BAD_VECTOR_SIZE.toApiException();
    }

    for (int i = 0; i < arrayNode.size(); i++) {
      JsonNode element = arrayNode.get(i);
      if (!element.isNumber()) {
        throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
      }
      arrayVals[i] = element.floatValue();
    }
    return arrayVals;
  }

  private String validateSortClausePath(String path) {
    if (!NamingRules.FIELD.apply(path)) {
      if (path.isEmpty()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
            "path must be represented as a non-empty string");
      }
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "path ('%s') cannot start with `$`", path);
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
