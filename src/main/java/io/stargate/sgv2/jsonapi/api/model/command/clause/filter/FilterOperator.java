package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.HashMap;
import java.util.Map;

/**
 * This interface will be implemented by Operator enum. Currently {@link ValueComparisonOperator} is
 * only implementation when array and sub doc data type are supported this will have multiple
 * implementation.
 */
public interface FilterOperator {
  String getOperator();

  class FilterOperatorUtils {
    private static Map<String, FilterOperator> operatorMap = new HashMap<>();

    static {
      for (FilterOperator filterOperator : ValueComparisonOperator.values()) {
        addComparisonOperator(filterOperator);
      }
      for (FilterOperator filterOperator : ElementComparisonOperator.values()) {
        addComparisonOperator(filterOperator);
      }
    }

    private static void addComparisonOperator(FilterOperator filterOperator) {
      operatorMap.put(filterOperator.getOperator(), filterOperator);
    }

    public static FilterOperator getComparisonOperator(String operator) {
      final FilterOperator filterOperator = operatorMap.get(operator);
      if (filterOperator == null)
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION, "Unsupported filter operation " + operator);

      return filterOperator;
    }
  }
}
