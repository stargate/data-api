package io.stargate.sgv2.jsonapi.service.operation.filters.table;

/** Filter to use any JSON number against a table column. */
public class NumberTableFilter extends NativeTypeTableFilter<Number> {

  public NumberTableFilter(String path, Operator operator, Number value) {
    super(path, operator, value);
  }
}
