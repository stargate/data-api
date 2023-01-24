package io.stargate.sgv3.docsapi.api.model.command.clause.update;

public enum UpdateOperator {
  SET("$set"),
  UNSET("unset");

  private String operator;

  UpdateOperator(String operator) {
    this.operator = operator;
  }
}
