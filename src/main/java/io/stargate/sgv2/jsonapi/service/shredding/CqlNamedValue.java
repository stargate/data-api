package io.stargate.sgv2.jsonapi.service.shredding;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodec;

/**
 * A value that can be sent to the CQL Driver or has come from it.
 *
 * <p>The value must be of the correct type for the column described by the {@link ColumnMetadata},
 * this means when handling values passed in they have gone through the {@link JSONCodec#toCQL()}
 * method.
 */
public class CqlNamedValue extends NamedValue<ColumnMetadata, Object> {

  public CqlNamedValue(ColumnMetadata name, Object value) {
    super(name, value);
  }
}
