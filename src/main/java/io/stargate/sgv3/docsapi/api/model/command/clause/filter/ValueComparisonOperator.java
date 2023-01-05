package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

/** List of value operator that can be used in Filter clause */
public enum ValueComparisonOperator implements FilterOperator {
  EQ,
  GT,
  GTE,
  LT,
  LTE,
  NE;
}
