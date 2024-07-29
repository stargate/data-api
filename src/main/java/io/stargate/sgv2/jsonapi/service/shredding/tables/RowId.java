package io.stargate.sgv2.jsonapi.service.shredding.tables;

import io.stargate.sgv2.jsonapi.service.shredding.DocRowIdentifer;

// TODO: AARON needs to hold all the values we identify as the parts of the PK for the row
public record RowId(Object[] values) implements DocRowIdentifer {

  @Override
  public Object value() {
    return values;
  }
}
