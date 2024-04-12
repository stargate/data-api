package io.stargate.sgv2.jsonapi.service.cql.builder;

public enum Predicate {
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
  ;

  private final String cql;

  Predicate(String cql) {
    this.cql = cql;
  }

  @Override
  public String toString() {
    return cql;
  }
}
