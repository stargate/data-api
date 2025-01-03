package io.stargate.sgv2.jsonapi.service.shredding.tables;

import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;

// TODO: AARON needs to hold all the values we identify as the parts of the PK for the row
public record RowId(Object[] values) implements DocRowIdentifer {
  public static final RowId EMPTY_ROWID = new RowId(new Object[0]);

  @Override
  public Object value() {
    return values;
  }
}
