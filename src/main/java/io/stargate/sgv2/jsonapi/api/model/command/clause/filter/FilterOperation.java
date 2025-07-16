package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import java.util.Set;

/**
 * A operation represent a filter condition, currently {@link ValueComparisonOperation} is the only
 * implementation, when Array and sub document types are added it will have multiple implementation.
 */
public interface FilterOperation<T> {

  /**
   * Check if the current filter operation matches the given operator and type.
   *
   * @param operators The set of operators to match against.
   * @param type The JsonType to match against.
   * @param appliesToTableMapSetList if true, the expression must be for a flagged as applying to
   *     table map/set/list column. We need an explicit flag because the operations to filter on
   *     map/set/list columns look the same as for a JSON array in a collection document.
   */
  boolean match(
      Set<? extends FilterOperator> operators, JsonType type, boolean appliesToTableMapSetList);

  FilterOperator operator();

  JsonLiteral<T> operand();

  /**
   * This method is used to get the mapSetListComponent of the filter operation.
   *
   * @return null if the FilterOperation is not against a table map/set/list column.
   */
  MapSetListFilterComponent mapSetListComponent();
}
