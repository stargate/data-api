package io.stargate.sgv2.jsonapi.util;

import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import java.util.List;
import java.util.stream.Collectors;

public class SortClauseUtil {
  public static List<FindOperation.OrderBy> resolveOrderBy(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return null;
    return sortClause.sortExpressions().stream()
        .map(
            sortExpression ->
                new FindOperation.OrderBy(sortExpression.path(), sortExpression.ascending()))
        .collect(Collectors.toList());
  }
}
