package io.stargate.sgv3.docsapi.commands.clauses.filter;

public class LogicalNotOperation extends LogicalUnaryOperation {
  public final UnaryLogicalOperation operator = UnaryLogicalOperation.NOT;
  public OperatorExpression operatorExpressions;
}
