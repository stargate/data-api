package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
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
    validateLogicalExpression(logicalExpression, indexingConfig);
  }

  public void validateLogicalExpression(
      LogicalExpression logicalExpression, CollectionSettings.IndexingConfig indexingConfig) {
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateLogicalExpression(subLogicalExpression, indexingConfig);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      validatePath(subComparisonExpression.getPath(), indexingConfig);
    }
  }

  public void validatePath(String path, CollectionSettings.IndexingConfig indexingConfig) {
    if (!indexingConfig.allowed().isEmpty()) {
      if (!indexingConfig.allowed().contains(path)) {
        throw new JsonApiException(
            ErrorCode.UNINDEXED_FILTER_PATH,
            String.format(
                "%s: The filter path ('%s') is not indexed",
                ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), path));
      }
    }
    if (!indexingConfig.denied().isEmpty()) {
      if (indexingConfig.denied().contains(path)) {
        throw new JsonApiException(
            ErrorCode.UNINDEXED_FILTER_PATH,
            String.format(
                "%s: The filter path ('%s') is not indexed",
                ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), path));
      }
    }
  }
}
