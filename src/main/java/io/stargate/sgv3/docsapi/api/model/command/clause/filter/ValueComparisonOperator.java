package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.util.HashMap;
import java.util.Map;

/** List of value operator that can be used in Filter clause */
public enum ValueComparisonOperator implements FilterOperator {
  EQ("$eq"),
  GT("$gt"),
  GTE("$gte"),
  LT("$lt"),
  LTE("$lte"),
  NE("$ne");

  private String operator;

  ValueComparisonOperator(String operator) {
    this.operator = operator;
  }

  private static Map<String, ValueComparisonOperator> operatorMap = new HashMap<>();

  static {
    for (ValueComparisonOperator filterOperator : ValueComparisonOperator.values()) {
      operatorMap.put(filterOperator.operator, filterOperator);
    }
  }

  public static ValueComparisonOperator getComparisonOperator(String operator) {
    return operatorMap.get(operator);
  }
}
