package io.stargate.sgv2.jsonapi.exception.checked;

import java.util.Optional;

/**
 * Base for checked exceptions we have in the code.
 *
 * <p>Use this for any exception that is declared in the method signature, in general we do not
 * expect these to be returned to the client, but rather to be caught and handled by the caller and
 * normally converted into a {@link io.stargate.sgv2.jsonapi.exception.APIException}
 */
public class CheckedApiException extends Exception {
  /**
   * Since String representation of some input values can be huge (like for Vectors), we limit the
   * length of the description to avoid flooding logs.
   */
  protected static final int MAX_VALUE_DESC_LENGTH = 1000;

  public CheckedApiException() {
    super();
  }

  public CheckedApiException(String message) {
    super(message);
  }

  public CheckedApiException(String message, Throwable cause) {
    super(message, cause);
  }

  public CheckedApiException(Throwable cause) {
    super(cause);
  }

  // Add a place to slightly massage value; can be further improved
  protected static String valueDesc(Object value) {
    if (value == null) {
      return "null";
    }
    String desc = maybeTruncate(String.valueOf(value));
    if (value instanceof String) {
      desc = "\"" + desc + "\"";
    }
    return desc;
  }

  protected static String className(Object value) {
    return Optional.of(value).map(Object::getClass).map(Class::getName).orElse("null");
  }

  private static String maybeTruncate(String value) {
    if (value.length() <= MAX_VALUE_DESC_LENGTH) {
      return value;
    }
    return value.substring(0, MAX_VALUE_DESC_LENGTH) + "[...](TRUNCATED)";
  }
}
