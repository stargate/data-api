package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** Interface Invertible implemented by comparisonExpression and logicalExpression */
public interface Invertible {

  /** method to invert a comparisonExpression or logicalExpression */
  Object invert();
}
