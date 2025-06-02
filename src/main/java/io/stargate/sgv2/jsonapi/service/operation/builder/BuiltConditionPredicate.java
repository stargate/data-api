package io.stargate.sgv2.jsonapi.service.operation.builder;

public enum BuiltConditionPredicate {
  EQ("="),
  NEQ("!="),
  LT("<"),
  GT(">"),
  LTE("<="),
  GTE(">="),
  IN("IN"),
  CONTAINS("CONTAINS"),
  NOT_CONTAINS("NOT CONTAINS"),
  CONTAINS_KEY("CONTAINS KEY"),
  TEXT_SEARCH(":");

  public final String cql;

  BuiltConditionPredicate(String cql) {
    this.cql = cql;
  }

  // TIDY - remove this use of toString() it should be used for log msg's etc, not core
  // functionality. This is called to build the CQL string we execute.
  @Override
  public String toString() {
    return cql;
  }
}
