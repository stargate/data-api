package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.SortClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
@JsonDeserialize(using = SortClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Map.class,
    example = """
              {"user.age" : -1, "user.name" : 1}
              """)
public record SortClause(@Valid List<SortExpression> sortExpressions) {

  public boolean hasVsearchClause() {
    return sortExpressions != null
        && !sortExpressions.isEmpty()
        && sortExpressions.get(0).path().equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
  }

  public boolean hasVectorizeSearchClause() {
    return sortExpressions != null
        && !sortExpressions.isEmpty()
        && sortExpressions
            .get(0)
            .path()
            .equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
  }

  public void validate(CommandContext commandContext) {
    DocumentProjector indexingProjector = commandContext.indexingProjector();
    // If nothing specified, everything indexed
    if (indexingProjector.equals(DocumentProjector.identityProjector())) {
      return;
    }
    // validate each path in sortExpressions
    for (SortExpression sortExpression : sortExpressions) {
      if (indexingProjector.isPathIncluded(sortExpression.path())) {
        throw new JsonApiException(
                ErrorCode.UNINDEXED_FILTER_PATH,
                String.format(
                        "%s: The filter path ('%s') is not indexed",
                        ErrorCode.UNINDEXED_FILTER_PATH.getMessage(), sortExpression.path()));
      }
    }
  }
}
