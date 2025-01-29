package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/**
 * This interface will be implemented by various comparison operator enums (like {@link
 * ArrayComparisonOperator}).
 */
public interface FilterOperator {
  String getOperator();

  /**
   * Flip comparison operator when `$not` is pushed down
   *
   * @return
   */
  FilterOperator invert();
}
