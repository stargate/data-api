package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import java.util.Optional;

/** Interface for commands that can have vector sort clause */
public interface VectorSortable extends Sortable {

  default Optional<Boolean> includeSimilarityScore() {
    return Optional.empty();
  }

  default Optional<Boolean> includeSortVector() {
    return Optional.empty();
  }

  default Optional<SortExpression> sortExpression() {
    if (sortClause() != null && !sortClause().tableVectorSorts().isEmpty()) {
      return Optional.of(sortClause().tableVectorSorts().getFirst());
    }
    return Optional.empty();
  }
}
