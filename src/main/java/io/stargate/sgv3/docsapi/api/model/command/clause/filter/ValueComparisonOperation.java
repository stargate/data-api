package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;
import javax.validation.constraints.NotNull;

/**
 * This object represents the operator and rhs operand of a filter clause
 *
 * @param operator Filter condition operator
 * @param operand Filter clause operand
 */
public record ValueComparisonOperation(
    @NotNull(message = "operator cannot be null") FilterOperator operator,
    @NotNull(message = "operand cannot be null") JsonLiteral operand)
    implements FilterOperation {

  @Override
  public boolean match(EnumSet operators, JsonType type) {
    return operators.contains(operator) && type.equals(operand.type());
  }
}
