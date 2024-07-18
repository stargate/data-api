package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import java.util.List;
import java.util.stream.Collectors;

// TIDY : rename or refactor to remove a "Util" class, this probably has a better home
public class SortClauseUtil {
  public static List<FindCollectionOperation.OrderBy> resolveOrderBy(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return null;
    if (sortClause.hasVsearchClause()) return null;
    return sortClause.sortExpressions().stream()
        .map(
            sortExpression ->
                new FindCollectionOperation.OrderBy(
                    sortExpression.path(), sortExpression.ascending()))
        .collect(Collectors.toList());
  }

  public static float[] resolveVsearch(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return null;
    if (sortClause.hasVsearchClause()) {
      return sortClause.sortExpressions().stream().findFirst().get().vector();
    }
    return null;
  }
}
