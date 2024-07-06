package io.stargate.sgv2.jsonapi.service.operation.model.impl.builder;

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
  ;

  private final String cql;

  BuiltConditionPredicate(String cql) {
    this.cql = cql;
  }

  @Override
  public String toString() {
    return cql;
  }
}
