package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
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

  /**
   * Returns the first SortExpression that returns true for {@link SortExpression#hasVector()}, if
   * there is more than one raises {@link IllegalStateException}.
   *
   * @return the vector sort expression if it exists.
   */
  default Optional<SortExpression> vectorSortExpression(CommandContext<?> ctx) {
    SortClause sortClause = sortClause(ctx);
    if (sortClause.sortExpressions() != null) {
      var vectorSorts =
          sortClause.sortExpressions().stream()
              .filter(expression -> expression.hasVector())
              .toList();
      if (vectorSorts.size() > 1) {
        throw new IllegalStateException("Only one vector sort expression is allowed");
      }
      return vectorSorts.isEmpty() ? Optional.empty() : Optional.of(vectorSorts.getFirst());
    }
    return Optional.empty();
  }
}
