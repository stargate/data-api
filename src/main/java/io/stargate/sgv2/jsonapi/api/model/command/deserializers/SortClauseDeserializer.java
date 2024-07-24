package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
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
      throw ErrorCode.INVALID_SORT_CLAUSE.toApiException(
          "Sort clause must be submitted as json object");
    }

    ObjectNode sortNode = (ObjectNode) node;

    // safe iterate, we know it's array
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    int totalFields = sortNode.size();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      String path = inner.getKey().trim();
      if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
        // Vector search can't be used with other sort clause
        if (totalFields > 1) {
          throw ErrorCode.VECTOR_SEARCH_USAGE_ERROR.toApiException();
        }
        if (!inner.getValue().isArray()) {
          throw ErrorCode.SHRED_BAD_VECTOR_VALUE.toApiException();
        } else {
          ArrayNode arrayNode = (ArrayNode) inner.getValue();
          float[] arrayVals = new float[arrayNode.size()];
          if (arrayNode.size() == 0) {
            throw ErrorCode.SHRED_BAD_VECTOR_SIZE.toApiException();
          }
          for (int i = 0; i < arrayNode.size(); i++) {
            JsonNode element = arrayNode.get(i);
            if (!element.isNumber()) {
              throw ErrorCode.SHRED_BAD_VECTOR_VALUE.toApiException();
            }
            arrayVals[i] = element.floatValue();
          }
          SortExpression exp = SortExpression.vsearch(arrayVals);
          sortExpressions.clear();
          sortExpressions.add(exp);
          break;
        }
      } else if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)) {
        // Vector search can't be used with other sort clause
        if (totalFields > 1) {
          throw ErrorCode.VECTOR_SEARCH_USAGE_ERROR.toApiException();
        }
        if (!inner.getValue().isTextual()) {
          throw ErrorCode.SHRED_BAD_VECTORIZE_VALUE.toApiException();
        }
        String vectorizeData = inner.getValue().textValue();
        if (vectorizeData.isBlank()) {
          throw ErrorCode.SHRED_BAD_VECTORIZE_VALUE.toApiException();
        }
        SortExpression exp = SortExpression.vectorizeSearch(vectorizeData);
        sortExpressions.clear();
        sortExpressions.add(exp);
        break;
      } else {
        if (path.isBlank()) {
          throw ErrorCode.INVALID_SORT_CLAUSE_PATH.toApiException(
              "sort clause path must be represented as not-blank string");
        }

        if (!DocumentConstants.Fields.VALID_PATH_PATTERN.matcher(path).matches()) {
          throw ErrorCode.INVALID_SORT_CLAUSE_PATH.toApiException(
              "sort clause path ('%s') contains character(s) not allowed", path);
        }

        if (!inner.getValue().isInt()
            || !(inner.getValue().intValue() == 1 || inner.getValue().intValue() == -1)) {
          throw ErrorCode.INVALID_SORT_CLAUSE_VALUE.toApiException(
              "Sort ordering value can only be `1` for ascending or `-1` for descending (not `%s`)",
              inner.getValue());
        }

        boolean ascending = inner.getValue().intValue() == 1;
        SortExpression exp = SortExpression.sort(path, ascending);
        sortExpressions.add(exp);
      }
    }
    return new SortClause(sortExpressions);
  }
}
