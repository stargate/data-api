package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import java.util.HashMap;
import java.util.Map;

/** Helper class for {@link FilterOperator} lookups. */
public class FilterOperators {
  private static Map<String, FilterOperator> operatorMap = new HashMap<>();

  static {
    for (FilterOperator filterOperator : ValueComparisonOperator.values()) {
      addComparisonOperator(filterOperator);
    }
    for (FilterOperator filterOperator : ElementComparisonOperator.values()) {
      addComparisonOperator(filterOperator);
    }
    for (FilterOperator filterOperator : ArrayComparisonOperator.values()) {
      addComparisonOperator(filterOperator);
    }
    // This should not be supported from outside
    operatorMap.remove(ArrayComparisonOperator.NOTANY.getOperator());
  }

  private static void addComparisonOperator(FilterOperator filterOperator) {
    operatorMap.put(filterOperator.getOperator(), filterOperator);
  }

  public static FilterOperator findComparisonOperator(String operator) {
    return operatorMap.get(operator);
  }
}
