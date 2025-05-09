package io.stargate.sgv2.jsonapi.exception.checked;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Checked exception thrown when we cannot find a codec for a column that matches the types we are
 * using.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class MissingJSONCodecException extends CheckedApiException {

  public final DataType dataType;
  public final Class<?> javaType;
  public final Object value;

  /**
   * Similar to {@link ToCQLCodecException}, no need to know the table, since this is exception is
   * not returned for the API and will be caught and handled into appropriate API error.
   */
  public MissingJSONCodecException(DataType dataType, Class<?> javaType, Object value) {
    super(
        String.format(
            "No JSONCodec found for cql dataType %s with java type %s and value %s",
            dataType, javaType, value));
    this.dataType = dataType;
    this.javaType = javaType;
    this.value = value;
  }
}
