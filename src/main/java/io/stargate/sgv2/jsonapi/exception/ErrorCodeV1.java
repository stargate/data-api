package io.stargate.sgv2.jsonapi.exception;

/**
 * ErrorCode is our deprecated internal enum that provides codes and a default message for that
 * error code.
 */
public enum ErrorCodeV1 {
  ;

  private final String message;

  ErrorCodeV1(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
