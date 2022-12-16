package io.stargate.sgv3.docsapi.commands.clauses.filter;

/**
 * <filter-logical-unary-operation> ::= <filter-logical-unary-operator-not>
 * <filter-logical-unary-operator-not> ::= $not <filter-operator-expression>
 */
public abstract class LogicalUnaryOperation extends FilterOperation {}
