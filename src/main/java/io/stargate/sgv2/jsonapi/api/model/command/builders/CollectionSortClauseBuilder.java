package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
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
}
