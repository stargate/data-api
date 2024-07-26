package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TableFilter;

import java.util.ArrayList;
import java.util.List;

public record TableWhereBuilder(TableSchemaObject table, LogicalExpression logicalExpression) implements WhereBuilder {

  @Override
  public Select apply(Select select, List<Object> positionalValues) {
    // BUG: this probably break order for nested expressions, for now enough to get this tested
    var tableFilters =
        logicalExpression.comparisonExpressions.stream()
            .flatMap(comparisonExpression -> comparisonExpression.getDbFilters().stream())
            .map(dbFilter -> (TableFilter) dbFilter)
            .toList();

    // Add the where clause operations
    for (TableFilter tableFilter : tableFilters) {
      select = tableFilter.apply(table, select, positionalValues);
    }
    return select;
  }
}
