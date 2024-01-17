package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonDeserialize(using = FilterClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example = """
             {"name": "Aaron", "country": "US"}
              """)
public record FilterClause(LogicalExpression logicalExpression) {
  public void validate(CommandContext commandContext) {
    DocumentProjector indexingProjector = commandContext.indexingProjector();
    // If nothing specified, everything indexed
    if (indexingProjector.isIdentityProjection()) {
      return;
    }
    validateLogicalExpression(logicalExpression, indexingProjector);
  }

  public void validateLogicalExpression(
      LogicalExpression logicalExpression, DocumentProjector indexingProjector) {
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateLogicalExpression(subLogicalExpression, indexingProjector);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      validateComparisonExpression(subComparisonExpression, indexingProjector);
    }
  }

  public void validateComparisonExpression(
      ComparisonExpression comparisonExpression, DocumentProjector indexingProjector) {
    String path = comparisonExpression.getPath();
    boolean isPathIndexed = indexingProjector.isPathIncluded(path);

    // If path is "_id" and it's denied, the operator can only be $eq or $in
    if (path.equals(DocumentConstants.Fields.DOC_ID) && !isPathIndexed) {
      FilterOperator filterOperator = comparisonExpression.getFilterOperations().get(0).operator();
      // if operator is not $eq or $in, throw error
      if (!filterOperator.equals(ValueComparisonOperator.EQ)
          && !filterOperator.equals(ValueComparisonOperator.IN)) {
        throw new JsonApiException(
            ErrorCode.ID_NOT_INDEXED,
            String.format(
                "%s: The filter path ('%s') is not indexed, you can only use $eq or $in as the operator",
                ErrorCode.ID_NOT_INDEXED.getMessage(), DocumentConstants.Fields.DOC_ID));
      } else {
        // else, _id can be used, return
        return;
      }
    }

    // If path is not indexed, throw error
    if (!isPathIndexed) {
      throw new JsonApiException(
          ErrorCode.UNINDEXED_FILTER_PATH,
          String.format(
              "%s: The filter path ('%s') is not indexed",
              ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), comparisonExpression.getPath()));
    }

    JsonLiteral<?> operand = comparisonExpression.getFilterOperations().get(0).operand();
    // If path is an object (like address), validate the incremental path (like address.city)
    if (operand.type() == JsonType.ARRAY || operand.type() == JsonType.SUB_DOC) {
      if (operand.value() instanceof Map<?, ?> map) {
        validateMap(indexingProjector, map, path);
      }
      if (operand.value() instanceof List<?> list) {
        validateList(indexingProjector, list, path);
      }
    }
  }

  private void validateMap(DocumentProjector indexingProjector, Map<?, ?> map, String currentPath) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() instanceof String) {
        String subPath = (String) entry.getKey();
        String incrementalPath = currentPath + "." + subPath;
        if (!indexingProjector.isPathIncluded(incrementalPath)) {
          throw new JsonApiException(
              ErrorCode.UNINDEXED_FILTER_PATH,
              String.format(
                  "%s: The filter path ('%s') is not indexed",
                  ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), incrementalPath));
        }
        // continue build the incremental path if the value is a map
        if (entry.getValue() instanceof Map<?, ?> valueMap) {
          validateMap(indexingProjector, valueMap, incrementalPath);
        }
        // continue build the incremental path if the value is a list
        if (entry.getValue() instanceof List<?> list) {
          validateList(indexingProjector, list, incrementalPath);
        }
      }
    }
  }

  private void validateList(DocumentProjector indexingProjector, List<?> list, String currentPath) {
    for (Object element : list) {
      if (element instanceof Map<?, ?> map) {
        validateMap(indexingProjector, map, currentPath);
      }
      if (element instanceof List<?> sublList) {
        validateList(indexingProjector, sublList, currentPath);
      }
      if (element instanceof String) {
        // no need to build incremental path, validate current path
        if (!indexingProjector.isPathIncluded(currentPath)) {
          throw new JsonApiException(
              ErrorCode.UNINDEXED_FILTER_PATH,
              String.format(
                  "%s: The filter path ('%s') is not indexed",
                  ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), currentPath));
        }
      }
    }
  }
}
