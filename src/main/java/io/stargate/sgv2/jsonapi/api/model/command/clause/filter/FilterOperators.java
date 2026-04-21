package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import java.util.HashMap;
import java.util.Map;

/** Helper class for {@link FilterOperator} lookups. */
public abstract class FilterOperators {
  private static final Map<String, FilterOperator> operatorMap;

  private FilterOperators() {}

  static {
    final Map<String, FilterOperator> ops = new HashMap<>();
    for (FilterOperator op : ValueComparisonOperator.values()) {
      ops.put(op.getOperator(), op);
    }
    for (FilterOperator op : ElementComparisonOperator.values()) {
      ops.put(op.getOperator(), op);
    }
    for (FilterOperator op : ArrayComparisonOperator.values()) {
      // This should not be supported from outside
      if (op != ArrayComparisonOperator.NOTANY) {
        ops.put(op.getOperator(), op);
      }
    }
    operatorMap = ops;
  }

  public static FilterOperator findComparisonOperator(String operator) {
    return operatorMap.get(operator);
  }
}
