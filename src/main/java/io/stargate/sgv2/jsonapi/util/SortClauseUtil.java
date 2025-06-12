package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import java.util.List;
import java.util.stream.Collectors;

// TIDY : rename or refactor to remove a "Util" class, this probably has a better home
public class SortClauseUtil {
  public static List<FindCollectionOperation.OrderBy> resolveOrderBy(SortClause sortClause) {
    if (sortClause == null || sortClause.isEmpty()) {
      return null;
    }
    if (sortClause.hasVsearchClause()) {
      return null;
    }
    // BM25 search is not supported via order by
    if (sortClause.lexicalSortExpression() != null) {
      return null;
    }
    return sortClause.sortExpressions().stream()
        .map(
            sortExpression ->
                new FindCollectionOperation.OrderBy(
                    sortExpression.getPath(), sortExpression.isAscending()))
        .collect(Collectors.toList());
  }

  public static float[] resolveVsearch(SortClause sortClause) {
    if (sortClause == null || sortClause.isEmpty()) {
      return null;
    }
    if (sortClause.hasVsearchClause()) {
      return sortClause.sortExpressions().stream().findFirst().get().getVector();
    }
    return null;
  }

  public static SortExpression resolveBM25Search(SortClause sortClause) {
    if (sortClause == null) {
      return null;
    }
    return sortClause.lexicalSortExpression();
  }
}
