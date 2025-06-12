package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.util.JsonUtil;

/** {@link SortClauseBuilder} to use with Collections. */
public class CollectionSortClauseBuilder extends SortClauseBuilder<CollectionSchemaObject> {
  public CollectionSortClauseBuilder(CollectionSchemaObject collection) {
    super(collection);
  }

  @Override
  public SortClause buildAndValidate(ObjectNode sortNode) {
    // $lexical is only special for Collections: handle first
    JsonNode lexicalNode = sortNode.get(DocumentConstants.Fields.LEXICAL_CONTENT_FIELD);
    if (lexicalNode != null) {
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
      if (!lexicalNode.isTextual()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
            "if sorting by '%s' value must be String, not %s",
            DocumentConstants.Fields.LEXICAL_CONTENT_FIELD, JsonUtil.nodeTypeAsString(lexicalNode));
      }
      return SortClause.immutable(SortExpression.collectionLexicalSort(lexicalNode.textValue()));
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

    // Otherwise, use shared default processing
    return super.buildAndValidate(sortNode);
  }

  @Override
  protected void validateSortClausePath(String path) {
    super.validateSortClausePath(path);
    if (!NamingRules.FIELD.apply(path)) {
      // Fail on empty (blank) and "$"-starting names (conflict with operators),
      // except allow "well-known" fields
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
  }
}
