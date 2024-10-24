package io.stargate.sgv2.jsonapi.exception.checked;

/**
 * Base for checked exceptions we have in the code.
 *
 * <p>Use this for any exception that is declared in the method signature, in general we do not
 * expect these to be returned to the client, but rather to be caught and handled by the caller and
 * normally converted into a {@link io.stargate.sgv2.jsonapi.exception.APIException}
 */
public class CheckedApiException extends Exception {

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
}
