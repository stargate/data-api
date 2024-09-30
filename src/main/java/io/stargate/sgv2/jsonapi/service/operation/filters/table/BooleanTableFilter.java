package io.stargate.sgv2.jsonapi.service.operation.filters.table;

/** Filter to use any JSON number against a table column. */
public class BooleanTableFilter extends NativeTypeTableFilter<Boolean> {

  public BooleanTableFilter(String path, Operator operator, Boolean value) {
    super(path, operator, value);
  }
}
