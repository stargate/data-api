package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.HashMap;
import java.util.Map;

/** This interface will be implemented by Operator enum. */
public interface FilterOperator {
  String getOperator();

  /**
   * Flip comparison operator when `$not` is pushed down
   *
   * @return
   */
  FilterOperator flip();

  class FilterOperatorUtils {
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
    }

    private static void addComparisonOperator(FilterOperator filterOperator) {
      operatorMap.put(filterOperator.getOperator(), filterOperator);
    }

    public static FilterOperator getComparisonOperator(String operator) {
      final FilterOperator filterOperator = operatorMap.get(operator);
      if (filterOperator == null)
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION, "Unsupported filter operator " + operator);

      return filterOperator;
    }
  }
}
