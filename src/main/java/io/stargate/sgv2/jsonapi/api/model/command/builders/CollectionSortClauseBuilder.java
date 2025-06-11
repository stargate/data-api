package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Collections;

/** {@link SortClauseBuilder} to use with Collections. */
public class CollectionSortClauseBuilder extends SortClauseBuilder<CollectionSchemaObject> {
  public CollectionSortClauseBuilder(CollectionSchemaObject collection) {
    super(collection);
  }

  @Override
  public SortClause buildAndValidate(ObjectNode sortNode) {
    // $lexical is only special for Collections: handle first
    JsonNode lexicalValue = sortNode.get(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    if (lexicalValue != null) {
      // We can also check if lexical sort supported by the collection:
      if (!schema.lexicalConfig().enabled()) {
        throw ErrorCodeV1.LEXICAL_NOT_ENABLED_FOR_COLLECTION.toApiException(
            "Lexical search is not enabled for collection '%s'", schema.name());
      }
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

      return new SortClause(
          Collections.singletonList(
              SortExpression.collectionLexicalSort(lexicalValue.textValue())));
    }

    // Otherwise, use shared default processing
    return defaultBuildAndValidate(sortNode);
  }

  @Override
  protected String validateSortClausePath(String path) {
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

  @Override
  protected SortExpression buildSortExpression(String path, JsonNode innerValue, int totalFields) {
    if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(path)) {
      // Vector search can't be used with other sort clause
      if (totalFields > 1) {
        throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
      }
      float[] vectorFloats = tryDecodeBinaryVector(path, innerValue);
      if (vectorFloats == null) {
        if (!innerValue.isArray()) {
          throw ErrorCodeV1.SHRED_BAD_VECTOR_VALUE.toApiException();
        }
        ArrayNode arrayNode = (ArrayNode) innerValue;
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
      return SortExpression.vsearch(vectorFloats);
    }
    if (DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(path)) {
      // Vector search can't be used with other sort clause
      if (totalFields > 1) {
        throw ErrorCodeV1.VECTOR_SEARCH_USAGE_ERROR.toApiException();
      }
      if (!innerValue.isTextual()) {
        throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
      }

      String vectorizeData = innerValue.textValue();
      if (vectorizeData.isBlank()) {
        throw ErrorCodeV1.SHRED_BAD_VECTORIZE_VALUE.toApiException();
      }
      return SortExpression.vectorizeSearch(vectorizeData);
    }
    return buildRegularSortExpression(path, innerValue);
  }
}
