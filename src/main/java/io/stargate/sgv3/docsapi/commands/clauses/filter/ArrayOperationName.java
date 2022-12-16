package io.stargate.sgv3.docsapi.commands.clauses.filter;

/**
 * <filter-array-operation> ::= <filter-array-operation-all> | <filter-array-operation-elemMatch> |
 * <filter-array-operation-size>
 */
public enum ArrayOperationName {
  ALL,
  ELEM_MATCH,
  SIZE
}
