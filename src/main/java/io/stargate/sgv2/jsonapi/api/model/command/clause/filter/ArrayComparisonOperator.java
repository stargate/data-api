package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/**
 * List of element level operator that can be used in Filter clause NOTANY operator is used
 * internally to support $not operation with $all
 */
public enum ArrayComparisonOperator implements FilterOperator {
  ALL("$all"),
  SIZE("$size"),
  /** Can not be used in filter clause operator in user api */
  NOTANY("$notany");

  private String operator;

  ArrayComparisonOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }

  @Override
  public FilterOperator flip() {
    switch (this) {
      case ALL:
        return NOTANY;
      case NOTANY:
        return ALL;
      default:
        return this;
    }
  }
}
