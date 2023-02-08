package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** List of element level operator that can be used in Filter clause */
public enum ArrayComparisonOperator implements FilterOperator {
  ALL("$all"),
  SIZE("$size");

  private String operator;

  ArrayComparisonOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }
}
