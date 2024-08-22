package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

/** Interface Invertible implemented by comparisonExpression and logicalExpression */
public interface Invertible {

  /** invert method to invert a comparisonExpression or logicalExpression */
  Invertible invert();
}
