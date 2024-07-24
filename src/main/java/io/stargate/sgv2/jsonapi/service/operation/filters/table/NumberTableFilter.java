package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import java.math.BigDecimal;

/** Filter to use any JSON number against a table column. */
public class NumberTableFilter extends NativeTypeTableFilter<BigDecimal> {

  public NumberTableFilter(String path, Operator operator, BigDecimal value) {
    super(path, operator, value);
  }
}
