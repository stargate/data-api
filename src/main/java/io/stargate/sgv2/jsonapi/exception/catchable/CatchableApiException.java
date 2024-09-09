package io.stargate.sgv2.jsonapi.exception.catchable;

/** Base for the catchable exceptions we have in the code */
public class CatchableApiException extends Exception {

  public CatchableApiException() {
    super();
  }

  public CatchableApiException(String message) {
    super(message);
  }

  public CatchableApiException(String message, Throwable cause) {
    super(message, cause);
  }

  public CatchableApiException(Throwable cause) {
    super(cause);
  }
}
