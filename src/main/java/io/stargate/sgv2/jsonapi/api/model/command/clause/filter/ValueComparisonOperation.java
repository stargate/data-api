package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import java.util.*;

/**
 * This object represents the operator and rhs operand of a filter clause
 *
 * @param operator Filter condition operator
 * @param operand Filter clause operand
 */
public record ValueComparisonOperation<T>(
    FilterOperator operator, JsonLiteral<T> operand, MapSetListFilterComponent mapSetListComponent)
    implements FilterOperation<T> {

  /**
   * Build a {@link ValueComparisonOperation} from the FilterOperator and operand node value from
   * request filter json. It is not against a table map/set/list column, so mapSetListComponent is
   * null.
   */
  public static ValueComparisonOperation<?> build(FilterOperator operator, Object operandValue) {
    return new ValueComparisonOperation<>(operator, JsonLiteral.wrap(operandValue), null);
  }

  /**
   * Build a {@link ValueComparisonOperation} from the FilterOperator and operand node value for a
   * specific map/set/list component.
   */
  public static ValueComparisonOperation<?> build(
      FilterOperator operator, Object operandValue, MapSetListFilterComponent mapSetListComponent) {
    return new ValueComparisonOperation<>(
        operator, JsonLiteral.wrap(operandValue), mapSetListComponent);
  }

  /** {@inheritDoc} */
  @Override
  public boolean match(
      Set<? extends FilterOperator> operators, JsonType type, boolean appliesToTableMapSetList) {

    // No need to check specific mapSetListComponent for table filtering feature.
    // Only checks if current FilterOperation has mapSetListComponent or not.
    if (appliesToTableMapSetList && mapSetListComponent == null) {
      // this expression is not for a table map/set/list column, but the caller wants it to be.
      return false;
    }
    if (!appliesToTableMapSetList && mapSetListComponent != null) {
      // this expression is for a table map/set/list column, but the caller does not want it to be.
      return false;
    }
    return operators.contains(operator) && type.equals(operand.type());
  }
}
