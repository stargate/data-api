package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.EnumSet;

public record ValueComparisonOperation(ValueComparisonOperator operator, JsonLiteral rhsOperand)
    implements FilterOperation {

  @Override
  public boolean match(EnumSet operator, JsonType type) {
    return operator.contains(operator) && type.equals(rhsOperand.type());
  }

  @Override
  public <T> T getTypedValue() {
    return rhsOperand.getTypedValue();
  }
}
