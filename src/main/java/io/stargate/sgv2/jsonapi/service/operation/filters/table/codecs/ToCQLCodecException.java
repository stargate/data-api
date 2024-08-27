package io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Checked exception thrown when we cannot convert a value to the type CQL expects.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class ToCQLCodecException extends Exception {

  public final Object value;
  public final DataType targetCQLType;

  public ToCQLCodecException(Object value, DataType targetCQLType, Exception cause) {
    super(
        String.format(
            "Error trying to convert to targetCQLType %s from value.class %s and value %s",
            targetCQLType, value.getClass().getName(), value),
        cause);
    this.value = value;
    this.targetCQLType = targetCQLType;
  }
}
