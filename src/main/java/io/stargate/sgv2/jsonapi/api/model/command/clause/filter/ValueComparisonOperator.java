package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.FilterException;
import java.util.Map;

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
    return switch (this) {
      case EQ -> NE;
      case NE -> EQ;
      case IN -> NIN;
      case NIN -> IN;
      case GT -> LTE;
      case GTE -> LT;
      case LT -> GTE;
      case LTE -> GT;
        // No way to do "not matches" (not supported by database)
      case MATCH ->
          throw FilterException.Code.FILTER_INVALID_EXPRESSION.get(
              Map.of("message", "cannot use '$not' to invert '$match' operator"));
    };
  }
}
