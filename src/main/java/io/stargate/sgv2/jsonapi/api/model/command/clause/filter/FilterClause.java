package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
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
  public void validate(CollectionSettings.IndexingConfig indexingConfig) {
    // If nothing specified, everything indexed
    if (indexingConfig == null) {
      return;
    }
    validateLogicalExpression(logicalExpression, indexingConfig);
  }

  public void validateLogicalExpression(
      LogicalExpression logicalExpression, CollectionSettings.IndexingConfig indexingConfig) {
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateLogicalExpression(subLogicalExpression, indexingConfig);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      validateComparisonExpression(subComparisonExpression, indexingConfig);
    }
  }

  public void validateComparisonExpression(
      ComparisonExpression comparisonExpression, CollectionSettings.IndexingConfig indexingConfig) {
    String path = comparisonExpression.getPath();
    // If _id is denied, the operator can only be $eq or $in
    if (path.equals(DocumentConstants.Fields.DOC_ID)) {
      if ((!indexingConfig.denied().isEmpty() && indexingConfig.denied().contains(DocumentConstants.Fields.DOC_ID))
          || (!indexingConfig.allowed().isEmpty() && !indexingConfig.allowed().contains(DocumentConstants.Fields.DOC_ID))
          || (!indexingConfig.denied().isEmpty()
              && indexingConfig.denied().iterator().next().equals("*"))) {
        FilterOperator filterOperator =
            comparisonExpression.getFilterOperations().get(0).operator();
        if (!(filterOperator.equals(ValueComparisonOperator.EQ))
            && !(filterOperator.equals(ValueComparisonOperator.IN))) {
          throw new JsonApiException(
              ErrorCode.ID_NOT_INDEXED,
              String.format(
                  "%s: The filter path ('%s') is not indexed, you can only use $eq or $in as the operator",
                  ErrorCode.ID_NOT_INDEXED.getMessage(), DocumentConstants.Fields.DOC_ID));
        }
      }
      return;
    }
    // If all fields are denied, throw error
    if (!indexingConfig.denied().isEmpty()
        && indexingConfig.denied().iterator().next().equals("*")) {
      throw new JsonApiException(
          ErrorCode.UNINDEXED_FILTER_PATH,
          String.format(
              "%s: All fields are not indexed, you can only use ('%s') in filter",
              ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), DocumentConstants.Fields.DOC_ID));
    }
    // Split the path into parts
    String[] pathParts = path.split("\\.");
    StringBuilder incrementalPath = new StringBuilder();
    // Check the path from high level to low level
    for (String part : pathParts) {
      // Construct the incremental path
      if (incrementalPath.length() > 0) {
        incrementalPath.append(".");
      }
      incrementalPath.append(part);

      // If allowed list exists - check if the incremental path is in the allowed paths
      if (!indexingConfig.allowed().isEmpty()
          && indexingConfig.allowed().contains(incrementalPath.toString())) {
        // Path is allowed, no need to check further
        return;
      }

      // If denied list exists - check if the incremental path is in the denied paths
      if (!indexingConfig.denied().isEmpty()
          && indexingConfig.denied().contains(incrementalPath.toString())) {
        // Path is denied, throw error
        throw new JsonApiException(
            ErrorCode.UNINDEXED_FILTER_PATH,
            String.format(
                "%s: The filter path ('%s') is not indexed",
                ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), path));
      }
    }

    // If allowed list exists - path is not in the allowed list
    if (!indexingConfig.allowed().isEmpty()) {
      throw new JsonApiException(
          ErrorCode.UNINDEXED_FILTER_PATH,
          String.format(
              "%s: The filter path ('%s') is not indexed",
              ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), path));
    }
  }
}
