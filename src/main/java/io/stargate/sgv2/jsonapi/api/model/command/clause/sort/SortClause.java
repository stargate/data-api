package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.SortClauseDeserializer;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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

  public boolean isEmpty() {
    return sortExpressions == null || sortExpressions.isEmpty();
  }

  /** Get the sort expressions that are trying to vector sort columns on a table */
  public List<SortExpression> tableVectorSorts() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream().filter(SortExpression::isTableVectorSort).toList();
  }

  /** Get the sort expressions that are not trying to vector sort columns on a table */
  public List<SortExpression> nonTableVectorSorts() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream()
            .filter(sortExpression -> !sortExpression.isTableVectorSort())
            .toList();
  }

  public List<CqlIdentifier> sortColumnIdentifiers() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream().map(SortExpression::pathAsCqlIdentifier).toList();
  }

  public List<SortExpression> tableVectorizeSorts() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream().filter(SortExpression::isTableVectorizeSort).toList();
  }

  /** Returns all non vector columns sorts. */
  public List<SortExpression> tableNonVectorSorts() {
    return sortExpressions.stream()
        .filter(
            sortExpression -> {
              return !sortExpression.isTableVectorSort();
            })
        .toList();
  }

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
      // `SortClauseDeserializer` looks for binary value and adds it as SortExpression irrespective
      // of field name to support ANN search for tables. There is no access to SchemaObject in the
      // deserializer, so added a validation to check in case of collection.

      if (!(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(sortExpression.path())
              || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(sortExpression.path()))
          && sortExpression.vector() != null) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
            "Sorting by embedding vector values for the collection requires `%s` field. Provided field name: `%s`.",
            DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, sortExpression.path());
      }
    }
  }
}
