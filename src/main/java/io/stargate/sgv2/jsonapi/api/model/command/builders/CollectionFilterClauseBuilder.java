package io.stargate.sgv2.jsonapi.api.model.command.builders;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.DocumentPath;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import java.util.List;
import java.util.Map;

public class CollectionFilterClauseBuilder extends FilterClauseBuilder<CollectionSchemaObject> {
  public CollectionFilterClauseBuilder(CollectionSchemaObject schema) {
    super(schema);
  }

  // Collections have fixed "_id" as THE document id
  @Override
  protected boolean isDocId(String path) {
    return DocumentConstants.Fields.DOC_ID.equals(path);
  }

  @Override
  protected FilterClause validateAndBuild(LogicalExpression rootExpr) {
    return new FilterClause(validateWithSchema(rootExpr));
  }

  @Override
  protected String validateFilterClausePath(String path) {
    if (!NamingRules.FIELD.apply(path)) {
      if (path.isEmpty()) {
        throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
            "filter clause path cannot be empty String");
      }
      // 3 special fields with $ prefix, skip here
      switch (path) {
        case DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
            DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD -> {
          return path;
        }
        case DocumentConstants.Fields.LEXICAL_CONTENT_FIELD -> {
          if (!schema.lexicalConfig().enabled()) {
            throw ErrorCodeV1.LEXICAL_NOT_ENABLED_FOR_COLLECTION.toApiException(
                "Lexical search is not enabled for collection '%s'", schema.name());
          }
          return path;
        }
      }
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "filter clause path ('%s') cannot start with `$`", path);
    }

    try {
      path = DocumentPath.verifyEncodedPath(path);
    } catch (IllegalArgumentException e) {
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "filter clause path ('%s') is not a valid path. " + e.getMessage(), path);
    }

    return path;
  }

  private LogicalExpression validateWithSchema(LogicalExpression rootExpr) {
    IndexingProjector indexingProjector = schema.indexingProjector();

    // If nothing specified, everything indexed
    if (!indexingProjector.isIdentityProjection()) {
      validateCollectionLogicalExpression(rootExpr, indexingProjector);
    }
    return rootExpr;
  }

  private void validateCollectionLogicalExpression(
      LogicalExpression logicalExpression, IndexingProjector indexingProjector) {
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateCollectionLogicalExpression(subLogicalExpression, indexingProjector);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      validateCollectionComparisonExpression(subComparisonExpression, indexingProjector);
    }
  }

  private void validateCollectionComparisonExpression(
      ComparisonExpression comparisonExpression, IndexingProjector indexingProjector) {
    String path = comparisonExpression.getPath();
    boolean isPathIndexed =
        !indexingProjector.isIndexingDenyAll() && indexingProjector.isPathIncluded(path);

    // special case path may be set to indexed for `$vector` and `$vectorize` fields even in case of
    // deny all

    if (!isPathIndexed) {
      // If path is "_id" and it's denied, the operator can only be $eq or $in
      if (path.equals(DocumentConstants.Fields.DOC_ID)) {
        FilterOperator filterOperator =
            comparisonExpression.getFilterOperations().get(0).operator();
        // if operator is $eq or $in, _id can be used, return
        if (filterOperator == ValueComparisonOperator.EQ
            || filterOperator == ValueComparisonOperator.IN) {
          return;
        }
        // otherwise throw _id - specific JsonApiException
        throw ErrorCodeV1.ID_NOT_INDEXED.toApiException(
            "you can only use $eq or $in as the operator");
      }
      // For any other not-indexed path throw generic error
      throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
          "filter path '%s' is not indexed", comparisonExpression.getPath());
    }

    JsonLiteral<?> operand = comparisonExpression.getFilterOperations().get(0).operand();
    // If path is an object (like address), validate the incremental path (like address.city)
    if (operand.type() == JsonType.ARRAY || operand.type() == JsonType.SUB_DOC) {
      if (operand.value() instanceof Map<?, ?> map) {
        validateCollectionMap(indexingProjector, map, path);
      }
      if (operand.value() instanceof List<?> list) {
        validateCollectionList(indexingProjector, list, path);
      }
    }
  }

  private void validateCollectionMap(
      IndexingProjector indexingProjector, Map<?, ?> map, String currentPath) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String incrementalPath = currentPath + "." + entry.getKey();
      if (!indexingProjector.isPathIncluded(incrementalPath)) {
        throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
            "filter path '%s' is not indexed", incrementalPath);
      }
      // continue build the incremental path if the value is a map
      if (entry.getValue() instanceof Map<?, ?> valueMap) {
        validateCollectionMap(indexingProjector, valueMap, incrementalPath);
      }
      // continue build the incremental path if the value is a list
      if (entry.getValue() instanceof List<?> list) {
        validateCollectionList(indexingProjector, list, incrementalPath);
      }
    }
  }

  private void validateCollectionList(
      IndexingProjector indexingProjector, List<?> list, String currentPath) {
    for (Object element : list) {
      if (element instanceof Map<?, ?> map) {
        validateCollectionMap(indexingProjector, map, currentPath);
      } else if (element instanceof List<?> sublList) {
        validateCollectionList(indexingProjector, sublList, currentPath);
      } else if (element instanceof String) {
        // no need to build incremental path, validate current path
        if (!indexingProjector.isPathIncluded(currentPath)) {
          throw ErrorCodeV1.UNINDEXED_FILTER_PATH.toApiException(
              "filter path '%s' is not indexed", currentPath);
        }
      }
    }
  }
}
