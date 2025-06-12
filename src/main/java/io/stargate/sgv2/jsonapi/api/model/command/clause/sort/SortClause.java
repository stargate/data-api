package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import jakarta.validation.Valid;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
public record SortClause(@Valid List<SortExpression> sortExpressions) {
  public SortClause {
    Objects.requireNonNull(sortExpressions, "sortExpressions cannot be null");
  }

  public static SortClause empty() {
    return new SortClause(Collections.emptyList());
  }

  public boolean isEmpty() {
    return sortExpressions.isEmpty();
  }

  /** Get the sort expressions that are trying to vector sort columns on a table */
  public List<SortExpression> tableVectorSorts() {
    return sortExpressions.isEmpty()
        ? List.of()
        : sortExpressions.stream()
            .filter(
                sortExpression ->
                    sortExpression.isTableVectorSort() || sortExpression.isTableVectorizeSort())
            .toList();
  }

  /** Get the sort expressions that are not trying to vector sort columns on a table */
  public List<SortExpression> nonTableVectorSorts() {
    return sortExpressions.isEmpty()
        ? List.of()
        : sortExpressions.stream()
            .filter(
                sortExpression ->
                    !sortExpression.isTableVectorSort() && !sortExpression.isTableVectorizeSort())
            .toList();
  }

  public List<CqlIdentifier> sortColumnIdentifiers() {
    return sortExpressions.isEmpty()
        ? List.of()
        : sortExpressions.stream().map(SortExpression::pathAsCqlIdentifier).toList();
  }

  public List<SortExpression> tableVectorizeSorts() {
    return sortExpressions.isEmpty()
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
    return !sortExpressions.isEmpty() && sortExpressions.getFirst().hasVector();
  }

  public boolean hasVectorizeSearchClause() {
    return !sortExpressions.isEmpty() && sortExpressions.getFirst().hasVectorize();
  }

  public SortExpression lexicalSortExpression() {
    if (sortExpressions.size() != 1) {
      return null;
    }
    SortExpression expr = sortExpressions.getFirst();
    return expr.isLexicalSort() ? expr : null;
  }

  public void validate(CollectionSchemaObject collection) {
    // First things first: Lexical/BM25 search uses its own index; also the only expression
    // (validated during SortClauseBuilder.buildAndValidate())
    if (lexicalSortExpression() != null) {
      return;
    }

    IndexingProjector indexingProjector = collection.indexingProjector();
    // If nothing specified, everything indexed
    if (indexingProjector.isIdentityProjection()) {
      return;
    }
    // validate each path in sortExpressions
    for (SortExpression sortExpression : sortExpressions) {
      if (!indexingProjector.isPathIncluded(sortExpression.getPath())) {
        throw ErrorCodeV1.UNINDEXED_SORT_PATH.toApiException(
            "sort path '%s' is not indexed", sortExpression.getPath());
      }
      // `SortClauseDeserializer` looks for binary value and adds it as SortExpression irrespective
      // of field name to support ANN search for tables. There is no access to SchemaObject in the
      // deserializer, so added a validation to check in case of collection.

      if (!(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(sortExpression.getPath())
              || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(
                  sortExpression.getPath()))
          && sortExpression.hasVector()) {
        throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
            "Sorting by embedding vector values for the collection requires `%s` field. Provided field name: `%s`.",
            DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, sortExpression.getPath());
      }
    }
  }
}
