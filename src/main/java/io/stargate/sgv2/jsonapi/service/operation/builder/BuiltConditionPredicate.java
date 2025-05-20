package io.stargate.sgv2.jsonapi.service.operation.builder;

public enum BuiltConditionPredicate {
  EQ("="),
  NEQ("!="),
  LT("<"),
  GT(">"),
  LTE("<="),
  GTE(">="),
  IN("IN"),
    TEXT_SEARCH(":"),
  CONTAINS("CONTAINS"),
  NOT_CONTAINS("NOT CONTAINS"),
  CONTAINS_KEY("CONTAINS KEY"),
  NOT_CONTAINS_KEY("NOT CONTAINS KEY");

  private final String cql;

  /**
   * Stores the CQL string representation of the predicate, wrapped with spaces on both sides. While
   * simple operators like "=" or "<" can work without surrounding spaces in CQL statements,
   * keywords such as "CONTAINS" or "IN" can cause syntax errors if not properly spaced. To ensure
   * consistency and avoid such issues, all predicates are uniformly wrapped with spaces.
   */
  private final String spaceWrappedCql;

  BuiltConditionPredicate(String cql) {
    this.cql = cql;
    this.spaceWrappedCql = " " + cql + " ";
  }

  /**
   * Returns the CQL representation of the predicate, with surrounding spaces. Use this public
   * method for building CQL statements.
   */
  public String getSpaceWrappedCql() {
    return spaceWrappedCql;
  }

  /**
   * Returns the CQL representation of the predicate without surrounding spaces, this is useful for
   * logs. To build the CQL statement, use {@link #getSpaceWrappedCql() instead.
   */
  @Override
  public String toString() {
    return spaceWrappedCql;
  }
}
