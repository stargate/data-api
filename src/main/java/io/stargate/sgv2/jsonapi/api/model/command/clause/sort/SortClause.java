package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.SortClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
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
    example =
        """
              {"user.age" : -1, "user.name" : 1}
              """)
public record SortClause(@Valid List<SortExpression> sortExpressions) implements SchemaValidatable {

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

  @Override
  public void validate(CollectionSchemaObject collection) {
    IndexingProjector indexingProjector = collection.indexingProjector();
    // If nothing specified, everything indexed
    if (indexingProjector.isIdentityProjection()) {
      return;
    }
    // validate each path in sortExpressions
    for (SortExpression sortExpression : sortExpressions) {
      if (!indexingProjector.isPathIncluded(sortExpression.path())) {
        throw ErrorCodeV1.UNINDEXED_SORT_PATH.toApiException(
            "sort path '%s' is not indexed", sortExpression.path());
      }
    }
  }
}
