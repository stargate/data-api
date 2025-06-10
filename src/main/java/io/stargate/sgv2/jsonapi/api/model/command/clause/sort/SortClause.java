package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.validation.Valid;
import java.util.Collections;
import java.util.List;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
public record SortClause(@Valid List<SortExpression> sortExpressions) {
  public static SortClause empty() {
    return new SortClause(Collections.emptyList());
  }

  public boolean isEmpty() {
    return sortExpressions == null || sortExpressions.isEmpty();
  }

  /** Get the sort expressions that are trying to vector sort columns on a table */
  public List<SortExpression> tableVectorSorts() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream()
            .filter(
                sortExpression ->
                    sortExpression.isTableVectorSort() || sortExpression.isTableVectorizeSort())
            .toList();
  }

  /** Get the sort expressions that are not trying to vector sort columns on a table */
  public List<SortExpression> nonTableVectorSorts() {
    return sortExpressions == null
        ? List.of()
        : sortExpressions.stream()
            .filter(
                sortExpression ->
                    !sortExpression.isTableVectorSort() && !sortExpression.isTableVectorizeSort())
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

  public SortExpression bm25SearchExpression() {
    if ((sortExpressions == null) || sortExpressions.size() != 1) {
      return null;
    }
    SortExpression expr = sortExpressions.get(0);
    return expr.isBM25Search() ? expr : null;
  }

  public void validate(CollectionSchemaObject collection) {
    // First things first: BM25 search uses its own index; also the only expression
    // (validated during SortClauseBuilder.buildAndValidate())
    if (bm25SearchExpression() != null) {
      return;
    }

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
