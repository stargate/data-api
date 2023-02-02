package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.JsonException;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.util.HashMap;
import java.util.Map;

/**
 * List of value operator that can be used in Filter clause Have commented the unsupported
 * operators, will add it as we support them
 */
public enum ValueComparisonOperator implements FilterOperator {
  EQ("$eq");
  /*GT("$gt"),
  GTE("$gte"),
  LT("$lt"),
  LTE("$lte"),
  NE("$ne");*/

  private String operator;

  ValueComparisonOperator(String operator) {
    this.operator = operator;
  }

  private static final Map<String, ValueComparisonOperator> operatorMap = new HashMap<>();

  static {
    for (ValueComparisonOperator filterOperator : ValueComparisonOperator.values()) {
      operatorMap.put(filterOperator.operator, filterOperator);
    }
  }

  public static ValueComparisonOperator getComparisonOperator(String operator) {
    final ValueComparisonOperator valueComparisonOperator = operatorMap.get(operator);
    if (valueComparisonOperator == null)
      throw new JsonException(
          ErrorCode.UNSUPPORTED_FILTER_OPERATION, "Unsupported filter operation " + operator);

    return valueComparisonOperator;
  }
}
