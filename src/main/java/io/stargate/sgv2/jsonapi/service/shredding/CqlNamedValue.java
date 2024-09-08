package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;

public class CqlNamedValue extends NamedValue<ColumnMetadata, Object> {

  public CqlNamedValue(ColumnMetadata name, Object value) {
    super(name, value);
  }
}
