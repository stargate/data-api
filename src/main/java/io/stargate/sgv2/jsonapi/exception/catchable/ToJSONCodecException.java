package io.stargate.sgv2.jsonapi.exception.catchable;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Checked exception thrown when we cannot convert a value to the type JSON expects.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class ToJSONCodecException extends CheckedApiException {

  public final Object value;
  public final DataType fromCqlType;

  public ToJSONCodecException(Object value, DataType fromCqlType, Exception cause) {
    super(formatMessage(value, fromCqlType, (cause == null) ? "NULL" : cause.getMessage()), cause);
    this.value = value;
    this.fromCqlType = fromCqlType;
  }

  public ToJSONCodecException(Object value, DataType fromCqlType, String rootCauseMessage) {
    super(formatMessage(value, fromCqlType, rootCauseMessage));
    this.value = value;
    this.fromCqlType = fromCqlType;
  }

  private static String formatMessage(Object value, DataType fromCqlType, String rootCauseMessage) {
    return String.format(
        "Error trying to convert fromCqlType `%s` from value.class `%s` and value: %s. Root cause: %s",
        fromCqlType, className(value), value);
  }
}
