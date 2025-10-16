package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** {@link SortClauseBuilder} to use with Collections. */
public class CollectionSortClauseBuilder extends SortClauseBuilder<CollectionSchemaObject> {
  public CollectionSortClauseBuilder(CollectionSchemaObject collection) {
    super(collection);
  }

  @Override
  public SortClause buildClauseFromDefinition(ObjectNode sortNode) {
    // Optimize for empty sort clause, which is common
    if (sortNode.isEmpty()) {
      return SortClause.empty();
    }
    // Start by checking "special" sort expressions, like lexical and vector sorts;
    // they must be the only expression in the sort clause.

    JsonNode lexicalNode = sortNode.get(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    if (lexicalNode != null) {
      // Typical we need a String, but "old" 1/-1 still allowed: so bail out
      // if we got a Number
      if (lexicalNode.isNumber()) {
        ; // do nothing, yet, fall-through to next block
      } else {
        // We can also check if lexical sort supported by the collection:
        if (!schema.lexicalConfig().enabled()) {
          throw SchemaException.Code.LEXICAL_NOT_ENABLED_FOR_COLLECTION.get(errVars(schema));
        }
        if (sortNode.size() > 1) {
          throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
              "if sorting by '%s' no other sort expressions allowed",
              DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
        }
        if (!lexicalNode.isTextual()) {
          throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
              "if sorting by '%s' value must be String, not %s",
              DocumentConstants.Fields.LEXICAL_CONTENT_FIELD,
              JsonUtil.nodeTypeAsString(lexicalNode));
        }
        return SortClause.immutable(SortExpression.collectionLexicalSort(lexicalNode.textValue()));
      }
    }
    JsonNode vectorNode = sortNode.get(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    if (vectorNode != null) {
      // Vector sort can't be used with other sort clauses
      if (sortNode.size() > 1) {
        throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
      }
      float[] vectorFloats =
          tryDecodeBinaryVector(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, vectorNode);
      if (vectorFloats == null) {
        if (!(vectorNode instanceof ArrayNode arrayNode)) {
          throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
        }
        vectorFloats = JsonUtil.arrayNodeToVector(arrayNode);
      }
      return SortClause.immutable(SortExpression.collectionVectorSort(vectorFloats));
    }

    JsonNode vectorizeNode = sortNode.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    if (vectorizeNode != null) {
      // Vectorize sort can't be used with other sort clauses
      if (sortNode.size() > 1) {
        throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
      }
      if (!vectorizeNode.isTextual()) {
        throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
      }

      String vectorizeData = vectorizeNode.textValue();
      if (vectorizeData.isBlank()) {
        throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
      }
      // 12-Jun-2025, tatu: Important! Due to original bad design, we need to allow
      //   modification of the enclosed SortExpression in this case, so:
      return SortClause.mutable(SortExpression.collecetionVectorizeSort(vectorizeData));
    }

    // Otherwise, only "regular" sort expressions are left
    // So let's validate the paths for the sort expressions
    validateSortExpressionPaths(sortNode);

    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());

    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();
      sortExpressions.add(buildRegularSortExpression(inner.getKey(), inner.getValue()));
    }
    return new SortClause(sortExpressions);
  }

  /**
   * Helper method to build a "non-special" sort expression for given definition; validates
   * expression value and builds the {@link SortExpression} object.
   *
   * @param path Path to the field to sort by, already validated
   * @param innerValue JSON value of the sort expression to use
   * @return {@link SortExpression} for the regular sort
   */
  private SortExpression buildRegularSortExpression(String path, JsonNode innerValue) {
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

  private void validateSortExpressionPaths(ObjectNode sortNode) {
    Iterator<String> it = sortNode.fieldNames();
    while (it.hasNext()) {
      validateSortExpressionPath(it.next());
    }
  }

  private void validateSortExpressionPath(String path) {
    if (!NamingRules.FIELD.apply(path)) {
      // This is only called after handling "special" sort expressions so simple validation
      if (path.isEmpty()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
            "path must be represented as a non-empty string");
      }
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "path ('%s') cannot start with '$' (except for pseudo-fields '$lexical', '$vector' and '$vectorize')",
          path);
    }

    try {
      DocumentPath.verifyEncodedPath(path);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE_PATH.toApiException(
          "sort clause path ('%s') is not a valid path. " + e.getMessage(), path);
    }
  }
}
