package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.api.model.command.table.MapSetListComponent;
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
   * @param toMatchMapSetListComponent Check mapSetListComponent is set or not.
   */
  boolean match(
      Set<? extends FilterOperator> operators, JsonType type, boolean toMatchMapSetListComponent);

  FilterOperator operator();

  JsonLiteral<T> operand();

  /**
   * This method is used to get the mapSetListComponent of the filter operation. Return null if the
   * FilterOperation is not against a table map/set/list column.
   */
  MapSetListComponent mapSetListComponent();
}
