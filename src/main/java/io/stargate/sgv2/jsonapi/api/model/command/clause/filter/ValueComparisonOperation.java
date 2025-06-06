package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import jakarta.validation.constraints.NotNull;
import java.util.EnumSet;

/**
 * This object represents the operator and rhs operand of a filter clause
 *
 * @param operator Filter condition operator
 * @param operand Filter clause operand
 */
public record ValueComparisonOperation<T>(
    @NotNull(message = "operator cannot be null") FilterOperator operator,
    @NotNull(message = "operand cannot be null") JsonLiteral<T> operand)
    implements FilterOperation<T> {

  @Override
  public boolean match(EnumSet<? extends FilterOperator> operators, JsonType type) {
    return operators.contains(operator) && type.equals(operand.type());
  }
}
