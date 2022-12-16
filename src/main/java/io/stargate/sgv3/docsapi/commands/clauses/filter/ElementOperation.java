package io.stargate.sgv3.docsapi.commands.clauses.filter;

/**
 * <filter-element-operation> ::= <filter-element-operation-exists>
 * <filter-element-operation-exists> ::= $exists <filter-element-operation-exists-operand>
 * <filter-element-operation-exists-operand> ::= true | false
 */
public abstract class ElementOperation extends FilterOperation {}
