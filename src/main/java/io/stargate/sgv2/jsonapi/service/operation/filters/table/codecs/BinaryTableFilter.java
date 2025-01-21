package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;

/** Filter to use any JSON Binary (EJSON/Base64-encoded) against a table column. */
public class BinaryTableFilter extends NativeTypeTableFilter<EJSONWrapper> {
  public BinaryTableFilter(String path, Operator operator, EJSONWrapper value) {
    super(path, operator, value);
  }
}
