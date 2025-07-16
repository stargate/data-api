package io.stargate.sgv2.jsonapi.service.operation.builder;

/**
 * aaron 25 june 2025 - this is from before the original move of the Data APi to use CQL driver, it
 * is part of the old custom-made query builder that we want to remove but it is still used in for
 * collections. It was updated with CONTAINS and some others to support filters on collection
 * columns that are not supported by the CQL driver.
 */
public enum BuiltConditionPredicate {
  CONTAINS("CONTAINS"),
  CONTAINS_KEY("CONTAINS KEY"),
  EQ("="),
  GT(">"),
  GTE(">="),
  IN("IN"),
  LT("<"),
  LTE("<="),
  NEQ("!="),
  NOT_CONTAINS("NOT CONTAINS"),
  NOT_CONTAINS_KEY("NOT CONTAINS KEY"),
  TEXT_SEARCH(":");

  /**
   * Stores the CQL string representation of the predicate, wrapped with spaces on both sides. While
   * simple operators like "=" or "<" can work without surrounding spaces in CQL statements,
   * keywords such as "CONTAINS" or "IN" can cause syntax errors if not properly spaced. To ensure
   * consistency and avoid such issues, all predicates are uniformly wrapped with spaces.
   */
  private final String spaceWrappedCql;

  BuiltConditionPredicate(String cql) {

    this.spaceWrappedCql = " " + cql + " ";
  }

  /**
   * Returns the CQL representation of the predicate, with surrounding spaces. Use this public
   * method for building CQL statements.
   */
  public String getCql() {
    return spaceWrappedCql;
  }

  /** Returns {@link #getCql()} */
  @Override
  public String toString() {
    return getCql();
  }
}
