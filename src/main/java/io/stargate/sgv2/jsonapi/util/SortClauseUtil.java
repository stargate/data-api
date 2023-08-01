package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import java.util.List;
import java.util.stream.Collectors;

public class SortClauseUtil {
  public static List<FindOperation.OrderBy> resolveOrderBy(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return null;
    if (hasVsearchClause(sortClause)) return null;
    return sortClause.sortExpressions().stream()
        .map(
            sortExpression ->
                new FindOperation.OrderBy(sortExpression.path(), sortExpression.ascending()))
        .collect(Collectors.toList());
  }

  public static float[] resolveVsearch(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return null;
    if (hasVsearchClause(sortClause)) {
      return sortClause.sortExpressions().stream().findFirst().get().vector();
    }
    return null;
  }

  private static boolean hasVsearchClause(SortClause sortClause) {
    return sortClause.sortExpressions().stream()
        .findFirst()
        .get()
        .path()
        .equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
  }
}
