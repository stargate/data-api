package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import java.util.List;

/**
 * <filter-logical-expression> ::= <filter-logical-compound-operator>
 * <filter-logical-compound-operand> <filter-logical-compound-operator> ::= $and, $or, $nor
 * <filter-logical-compound-operand> ::= [<filter-expression> (, <filter-expression>)]*
 */
public class LogicalExpression {
  public LogicalOperator operator;
  public List<FilterClause.FilterExpression> expressions;
}
