package io.stargate.sgv2.jsonapi.service.operation.filters.table;

/**
 * Filter to use any JSON string value against a table column.

 */
public class TextTableFilter extends NativeTypeTableFilter<String> {

  public TextTableFilter(String path, Operator operator, String value) {
    super(path, operator, value);
  }
}
