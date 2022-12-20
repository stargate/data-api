package io.stargate.sgv3.docsapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {

  /** Command error codes. */
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented.");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }
}
