package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/**
 * List of value operator that can be used in Filter clause Have commented the unsupported
 * operators, will add it as we support them
 */
public enum ValueComparisonOperator implements FilterOperator {
  EQ("$eq"),
  IN("$in");
  /*GT("$gt"),
  GTE("$gte"),
  LT("$lt"),
  LTE("$lte"),
  NE("$ne");*/

  private String operator;

  ValueComparisonOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }
}
