package io.stargate.sgv2.jsonapi.exception.catchable;

import com.datastax.oss.driver.api.core.type.DataType;

/**
 * Checked exception thrown when we cannot convert a value to the type CQL expects.
 *
 * <p>Not intended to be returned on the API, usage of the JSONCodec's should catch this and turn it
 * into the appropriate API error.
 */
public class ToCQLCodecException extends CheckedApiException {
  /**
   * Since String representation of some input values can be huge (like for Vectors), we limit the
   * length of the description to avoid flooding logs.
   */
  private static final int MAX_VALUE_DESC_LENGTH = 1000;

  public final Object value;
  public final DataType targetCQLType;

  public ToCQLCodecException(Object value, DataType targetCQLType, Exception rootCause) {
    super(
        formatMessage(value, targetCQLType, (rootCause == null) ? "NULL" : rootCause.getMessage()),
        rootCause);
    this.value = value;
    this.targetCQLType = targetCQLType;
  }

  public ToCQLCodecException(Object value, DataType targetCQLType, String rootCauseMessage) {
    super(formatMessage(value, targetCQLType, rootCauseMessage));
    this.value = value;
    this.targetCQLType = targetCQLType;
  }

  private static String formatMessage(
      Object value, DataType targetCQLType, String rootCauseMessage) {
    return String.format(
        "Error trying to convert to targetCQLType `%s` from value.class `%s`, value %s. Root cause: %s",
        targetCQLType, className(value), valueDesc(value), rootCauseMessage);
  }

  // Add a place to slightly massage value; can be further improved
  private static String valueDesc(Object value) {
    if (value == null) {
      return "null";
    }
    String desc = maybeTruncate(String.valueOf(value));
    if (value instanceof String) {
      desc = "\"" + desc + "\"";
    }
    return desc;
  }

  private static String className(Object value) {
    if (value == null) {
      return "null";
    }
    return value.getClass().getName();
  }

  private static String maybeTruncate(String value) {
    if (value.length() <= MAX_VALUE_DESC_LENGTH) {
      return value;
    }
    return value.substring(0, MAX_VALUE_DESC_LENGTH) + "[...](TRUNCATED)";
  }
}
