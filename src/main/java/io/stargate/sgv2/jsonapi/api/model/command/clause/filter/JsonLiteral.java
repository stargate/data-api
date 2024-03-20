package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

// Literal value to use as RHS operand in the query

import java.math.BigDecimal;

/**
 * @param value Literal value to use as RHS operand in the query
 * @param type Literal value type of the RHS operand in the query
 * @param <T> Data type of the object
 */
public record JsonLiteral<T>(T value, JsonType type) {

  // for negating 0, will set BigDecimal -0.5 as flag
  // this is used when pushing down "$not" to {"$size":0}
  public static final BigDecimal NEGATE_ZERO = new BigDecimal("-1.5");
}
