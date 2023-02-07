package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** List of element level operator that can be used in Filter clause */
public enum ElementComparisonOperator implements FilterOperator {
  EXISTS("$exists");

  private String operator;

  ElementComparisonOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }
}
