package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;
import javax.validation.constraints.NotNull;

/**
 * This object represents the operator and rhs operand of a filter clause
 *
 * @param operator
 * @param rhsOperand
 */
public record ValueComparisonOperation(
    @NotNull(message = "operator cannot be null") FilterOperator operator,
    @NotNull(message = "operand cannot be null") JsonLiteral rhsOperand)
    implements FilterOperation {

  @Override
  public boolean match(EnumSet operator, JsonType type) {
    return operator.contains(operator) && type.equals(rhsOperand.type());
  }

  @Override
  public FilterOperator getOperator() {
    return operator;
  }

  @Override
  public JsonLiteral getOperand() {
    return rhsOperand;
  }
}
