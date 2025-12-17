package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;

/**
 * List of value operator that can be used in Filter clause Have commented the unsupported
 * operators, will add it as we support them
 */
public enum ValueComparisonOperator implements FilterOperator {
  EQ("$eq"),
  NE("$ne"),
  IN("$in"),
  NIN("$nin"),
  GT("$gt"),
  GTE("$gte"),
  LT("$lt"),
  LTE("$lte"),
  MATCH("$match");

  private String operator;

  ValueComparisonOperator(String operator) {
    this.operator = operator;
  }

  @Override
  public String getOperator() {
    return operator;
  }

  @Override
  public FilterOperator invert() {
    switch (this) {
      case EQ:
        return NE;
      case NE:
        return EQ;
      case IN:
        return NIN;
      case NIN:
        return IN;
      case GT:
        return LTE;
      case GTE:
        return LT;
      case LT:
        return GTE;
      case LTE:
        return GT;
      case MATCH:
        // No way to do "not matches" (not supported by database)
        throw ErrorCodeV1.FILTER_INVALID_EXPRESSION.toApiException(
            "cannot use $not to invert $match operator");
    }
    return this;
  }
}
